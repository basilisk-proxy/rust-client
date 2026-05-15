use crate::error::{ClientError, ClientResult};
use crate::protocol::{
    ServiceBusEventEnvelope, ServiceBusForwardRequest, ServiceBusForwardResponse,
    ServiceBusProtocolMessage, protocol_types,
};
use chrono::Utc;
use futures_util::{FutureExt, future::BoxFuture};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, RwLock, oneshot};
use tokio::time::{Duration, timeout};

const COMMAND_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const METRICS_INTERVAL: Duration = Duration::from_secs(20);
const CONNECT_RETRY_BASE_DELAY: Duration = Duration::from_millis(500);
const CONNECT_RETRY_MAX_DELAY: Duration = Duration::from_secs(30);
const CONNECT_RETRY_MAX_JITTER_MS: u64 = 500;
const CONNECT_RETRY_MAX_ATTEMPTS: usize = 8;
const METRICS_TOPIC: &str = "basilisk.metrics.distribution";
const METRICS_MESSAGE_TYPE: &str = "basilisk.internal";

pub type EventHandler =
    Arc<dyn Fn(ServiceBusEventEnvelope) -> BoxFuture<'static, ()> + Send + Sync>;
pub type RequestHandler = Arc<
    dyn Fn(ServiceBusRequest, RequestResponder) -> BoxFuture<'static, ClientResult<()>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct BusClient {
    inner: Arc<Inner>,
}

struct Inner {
    service_id: String,
    instance_id: String,
    writer: Mutex<OwnedWriteHalf>,
    pending: Mutex<VecDeque<oneshot::Sender<ServiceBusProtocolMessage>>>,
    event_handlers: RwLock<HashMap<String, Vec<EventHandler>>>,
    request_handlers: RwLock<HashMap<String, RequestHandler>>,
}

#[derive(Debug, Clone)]
pub struct ServiceBusRequest {
    pub event: ServiceBusEventEnvelope,
}

impl ServiceBusRequest {
    pub fn reply_to(&self) -> Option<&str> {
        self.event.payload.get("reply_to")?.as_str()
    }
}

#[derive(Clone)]
pub struct RequestResponder {
    client: BusClient,
    reply_to_topic: String,
    causation_id: String,
    correlation_id: i64,
    default_message_type: String,
}

impl RequestResponder {
    pub async fn respond(
        &self,
        message_type: impl Into<String>,
        payload: HashMap<String, serde_json::Value>,
    ) -> ClientResult<i32> {
        let event = ServiceBusEventEnvelope {
            event_id: String::new(),
            emitted_at_utc: Utc::now(),
            service_id: String::new(),
            instance_id: String::new(),
            topic: self.reply_to_topic.clone(),
            message_type: message_type.into(),
            correlation_id: self.correlation_id,
            causation_id: Some(self.causation_id.clone()),
            payload,
        };
        self.client.publish_event(event).await
    }

    pub async fn respond_ok(
        &self,
        payload: HashMap<String, serde_json::Value>,
    ) -> ClientResult<i32> {
        self.respond(self.default_message_type.clone(), payload)
            .await
    }
}

#[derive(Debug, Clone)]
pub struct ForwardRequest {
    pub target_service_id: String,
    pub message_type: String,
    pub payload: HashMap<String, serde_json::Value>,
    pub timeout_ms: Option<u64>,
}

impl BusClient {
    pub async fn connect(
        host: &str,
        port: u16,
        service_id: impl Into<String>,
        instance_id: impl Into<String>,
        token: impl Into<String>,
    ) -> ClientResult<Self> {
        let service_id = service_id.into();
        let instance_id = instance_id.into();
        let token = token.into();
        let connection_key = format!("{}:{}", service_id, instance_id);

        let mut attempt = 0usize;
        loop {
            let stream = match TcpStream::connect((host, port)).await {
                Ok(stream) => stream,
                Err(err) => {
                    eprintln!("[basilisk][{connection_key}] tcp connect failed: {err}");
                    if attempt >= CONNECT_RETRY_MAX_ATTEMPTS - 1 {
                        eprintln!("[basilisk][{connection_key}] giving up after retries");
                        return Err(err.into());
                    }
                    let delay = connect_retry_delay(attempt);
                    eprintln!("[basilisk][{connection_key}] retry scheduled in {:?}", delay);
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                    continue;
                }
            };

            let _ = stream.set_nodelay(true);
            let (reader, writer) = stream.into_split();

            let inner = Arc::new(Inner {
                service_id: service_id.clone(),
                instance_id: instance_id.clone(),
                writer: Mutex::new(writer),
                pending: Mutex::new(VecDeque::new()),
                event_handlers: RwLock::new(HashMap::new()),
                request_handlers: RwLock::new(HashMap::new()),
            });

            tokio::spawn(read_loop(Arc::clone(&inner), reader));

            eprintln!("[basilisk][{connection_key}] tcp socket established; sending connect handshake");

            let client = Self {
                inner: Arc::clone(&inner),
            };

            let connect_result = client
                .send_command(ServiceBusProtocolMessage {
                    r#type: protocol_types::CONNECT.to_string(),
                    service_id: Some(service_id.clone()),
                    instance_id: Some(instance_id.clone()),
                    token: Some(token.clone()),
                    ..Default::default()
                })
                .await;

            if let Err(err) = connect_result {
                eprintln!("[basilisk][{connection_key}] connect handshake failed: {err}");
                if attempt >= CONNECT_RETRY_MAX_ATTEMPTS - 1 {
                    return Err(err);
                }
                let delay = connect_retry_delay(attempt);
                eprintln!("[basilisk][{connection_key}] retry scheduled in {:?}", delay);
                tokio::time::sleep(delay).await;
                attempt += 1;
                continue;
            }

            eprintln!("[basilisk][{connection_key}] authenticated and connected");

            start_metrics_publisher(client.clone());
            return Ok(client);
        }
    }

    pub async fn subscribe(&self, topics: Vec<String>) -> ClientResult<()> {
        self.send_command(ServiceBusProtocolMessage {
            r#type: protocol_types::SUBSCRIBE.to_string(),
            topics: Some(topics),
            ..Default::default()
        })
        .await
        .map(|_| ())
    }

    pub async fn unsubscribe(&self, topics: Vec<String>) -> ClientResult<()> {
        self.send_fire_and_forget(ServiceBusProtocolMessage {
            r#type: protocol_types::UNSUBSCRIBE.to_string(),
            topics: Some(topics),
            ..Default::default()
        })
        .await
    }

    pub async fn publish(
        &self,
        topic: impl Into<String>,
        message_type: impl Into<String>,
        payload: HashMap<String, serde_json::Value>,
    ) -> ClientResult<i32> {
        let event = ServiceBusEventEnvelope {
            event_id: String::new(),
            emitted_at_utc: Utc::now(),
            service_id: String::new(),
            instance_id: String::new(),
            topic: topic.into(),
            message_type: message_type.into(),
            correlation_id: 0,
            causation_id: None,
            payload,
        };

        self.publish_event(event).await
    }

    pub async fn publish_event(&self, event: ServiceBusEventEnvelope) -> ClientResult<i32> {
        let msg = self
            .send_command(ServiceBusProtocolMessage {
                r#type: protocol_types::PUBLISH.to_string(),
                event: Some(event),
                ..Default::default()
            })
            .await?;

        Ok(msg.subscriber_count.unwrap_or_default())
    }

    pub async fn forward(
        &self,
        request: ForwardRequest,
    ) -> ClientResult<ServiceBusForwardResponse> {
        let response = self
            .send_command_expect(ServiceBusProtocolMessage {
                r#type: protocol_types::FORWARD.to_string(),
                forward_request: Some(ServiceBusForwardRequest {
                    target_service_id: request.target_service_id,
                    message_type: request.message_type,
                    payload: request.payload,
                    timeout_ms: request.timeout_ms,
                }),
                ..Default::default()
            })
            .await?;

        if response.r#type != protocol_types::FORWARD_RESPONSE {
            return Err(ClientError::UnexpectedMessage(response.r#type));
        }

        response
            .forward_response
            .ok_or(ClientError::MissingField("forwardResponse"))
    }

    pub async fn on_event<F, Fut>(&self, topic: impl Into<String>, handler: F) -> ClientResult<()>
    where
        F: Fn(ServiceBusEventEnvelope) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let topic = topic.into();
        self.subscribe(vec![topic.clone()]).await?;

        let boxed: EventHandler = Arc::new(move |event| handler(event).boxed());
        let mut guard = self.inner.event_handlers.write().await;
        guard.entry(topic).or_default().push(boxed);
        Ok(())
    }

    pub async fn on_request<F, Fut>(
        &self,
        topic: impl Into<String>,
        responder: F,
    ) -> ClientResult<()>
    where
        F: Fn(ServiceBusRequest, RequestResponder) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ClientResult<()>> + Send + 'static,
    {
        let topic = topic.into();
        let service_topic = format!("service-{}", self.inner.service_id);
        self.subscribe(vec![service_topic]).await?;

        let handler: RequestHandler = Arc::new(move |req, resp| responder(req, resp).boxed());
        let mut guard = self.inner.request_handlers.write().await;
        guard.insert(topic, handler);
        Ok(())
    }

    async fn send_command(
        &self,
        msg: ServiceBusProtocolMessage,
    ) -> ClientResult<ServiceBusProtocolMessage> {
        let response = self.send_command_expect(msg).await?;
        if response.r#type != protocol_types::ACK {
            return Err(ClientError::UnexpectedMessage(response.r#type));
        }
        Ok(response)
    }

    async fn send_command_expect(
        &self,
        msg: ServiceBusProtocolMessage,
    ) -> ClientResult<ServiceBusProtocolMessage> {
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.inner.pending.lock().await;
            pending.push_back(tx);
        }

        let mut wire = serde_json::to_string(&msg)?;
        wire.push('\n');
        let mut writer = self.inner.writer.lock().await;
        if let Err(err) = writer.write_all(wire.as_bytes()).await {
            let mut pending = self.inner.pending.lock().await;
            let _ = pending.pop_back();
            return Err(err.into());
        }
        writer.flush().await?;

        let response = timeout(COMMAND_RESPONSE_TIMEOUT, rx)
            .await
            .map_err(|_| ClientError::Protocol {
                code: "COMMAND_TIMEOUT".to_string(),
                message: "Timed out waiting for protocol response".to_string(),
            })?
            .map_err(|_| ClientError::ChannelClosed)?;
        if response.r#type == protocol_types::ERROR {
            return Err(ClientError::Protocol {
                code: response
                    .error_code
                    .unwrap_or_else(|| "UNKNOWN_ERROR".to_string()),
                message: response
                    .message
                    .unwrap_or_else(|| "Service bus protocol error".to_string()),
            });
        }

        Ok(response)
    }

    async fn send_fire_and_forget(&self, msg: ServiceBusProtocolMessage) -> ClientResult<()> {
        let mut wire = serde_json::to_string(&msg)?;
        wire.push('\n');
        let mut writer = self.inner.writer.lock().await;
        writer.write_all(wire.as_bytes()).await?;
        writer.flush().await?;
        Ok(())
    }
}

fn start_metrics_publisher(client: BusClient) {
    let weak_inner = Arc::downgrade(&client.inner);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(METRICS_INTERVAL);
        loop {
            ticker.tick().await;
            let Some(inner) = weak_inner.upgrade() else {
                eprintln!("[basilisk] metrics task stopping because client was dropped");
                break;
            };

            let metrics_client = BusClient { inner };
            let connection_key = format!(
                "{}:{}",
                metrics_client.inner.service_id, metrics_client.inner.instance_id
            );
            match metrics_client
                .publish(METRICS_TOPIC, METRICS_MESSAGE_TYPE, build_metrics_payload())
                .await
            {
                Ok(subscribers) => {
                    eprintln!("[basilisk][{connection_key}] metric published subscribers={subscribers}");
                }
                Err(err) => {
                    eprintln!("[basilisk][{connection_key}] metric publish failed: {err}");
                }
            }
        }
    });
}

fn build_metrics_payload() -> HashMap<String, serde_json::Value> {
    let mut payload = HashMap::new();
    payload.insert(
        "name".to_string(),
        serde_json::Value::String("memory_usage".to_string()),
    );
    payload.insert(
        "value".to_string(),
        serde_json::Value::from(current_memory_usage_bytes()),
    );
    payload.insert(
        "unit".to_string(),
        serde_json::Value::String("bytes".to_string()),
    );
    payload
}

fn current_memory_usage_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/statm")
            && let Some(pages_str) = status.split_whitespace().next()
            && let Ok(pages) = pages_str.parse::<u64>()
        {
            return pages.saturating_mul(4096);
        }
    }

    0
}

fn connect_retry_delay(attempt: usize) -> Duration {
    let exp_factor = 1u128 << attempt.min(16);
    let base_ms = CONNECT_RETRY_BASE_DELAY
        .as_millis()
        .saturating_mul(exp_factor);
    let capped_ms = base_ms.min(CONNECT_RETRY_MAX_DELAY.as_millis()) as u64;
    let jitter = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0))
        % (CONNECT_RETRY_MAX_JITTER_MS + 1);
    Duration::from_millis(capped_ms.saturating_add(jitter))
}

async fn read_loop(inner: Arc<Inner>, reader: OwnedReadHalf) {
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let connection_key = format!("{}:{}", inner.service_id, inner.instance_id);

    eprintln!("[basilisk][{connection_key}] read loop started");

    loop {
        line.clear();
        let bytes = match reader.read_line(&mut line).await {
            Ok(size) => size,
            Err(err) => {
                eprintln!("[basilisk][{connection_key}] read error: {err}");
                break;
            }
        };
        if bytes == 0 {
            eprintln!("[basilisk][{connection_key}] socket closed by peer");
            break;
        }

        let message: ServiceBusProtocolMessage = match serde_json::from_str(&line) {
            Ok(msg) => msg,
            Err(err) => {
                eprintln!("[basilisk][{connection_key}] failed to parse message: {err}");
                continue;
            }
        };

        match message.r#type.as_str() {
            protocol_types::EVENT => {
                if let Some(event) = message.event {
                    dispatch_event(Arc::clone(&inner), event).await;
                }
            }
            protocol_types::ACK | protocol_types::ERROR | protocol_types::FORWARD_RESPONSE => {
                let sender = {
                    let mut pending = inner.pending.lock().await;
                    pending.pop_front()
                };
                if let Some(sender) = sender {
                    let _ = sender.send(message);
                }
            }
            _ => {}
        }
    }

    eprintln!("[basilisk][{connection_key}] read loop stopped");
}

async fn dispatch_event(inner: Arc<Inner>, event: ServiceBusEventEnvelope) {
    let handlers = {
        let guard = inner.event_handlers.read().await;
        let mut collected: Vec<EventHandler> = guard.get(&event.topic).cloned().unwrap_or_default();
        if let Some(wildcard) = guard.get("*") {
            collected.extend(wildcard.iter().cloned());
        }
        collected
    };

    for handler in handlers {
        let event_clone = event.clone();
        tokio::spawn(async move {
            handler(event_clone).await;
        });
    }

    if event.topic == format!("service-{}", inner.service_id) {
        let request_handler = {
            let guard = inner.request_handlers.read().await;
            guard.get(&event.message_type).cloned()
        };

        if let Some(handler) = request_handler
            && let Some(reply_to) = event.payload.get("reply_to").and_then(|v| v.as_str())
        {
            let req = ServiceBusRequest {
                event: event.clone(),
            };
            let responder = RequestResponder {
                client: BusClient {
                    inner: Arc::clone(&inner),
                },
                reply_to_topic: reply_to.to_string(),
                causation_id: event.event_id.clone(),
                correlation_id: event.correlation_id,
                default_message_type: event.message_type.clone(),
            };

            tokio::spawn(async move {
                let _ = handler(req, responder).await;
            });
        }
    }
}
