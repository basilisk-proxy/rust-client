use crate::error::{ClientError, ClientResult};
use crate::protocol::{
    ServiceBusEventEnvelope, ServiceBusForwardRequest, ServiceBusForwardResponse,
    ServiceBusProtocolMessage, protocol_types,
};
use chrono::Utc;
use futures_util::{FutureExt, future::BoxFuture};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, RwLock, oneshot};
use tokio::time::{Duration, timeout};

const COMMAND_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

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

        let stream = TcpStream::connect((host, port)).await?;
        let (reader, writer) = stream.into_split();

        let inner = Arc::new(Inner {
            service_id: service_id.clone(),
            writer: Mutex::new(writer),
            pending: Mutex::new(VecDeque::new()),
            event_handlers: RwLock::new(HashMap::new()),
            request_handlers: RwLock::new(HashMap::new()),
        });

        let client = Self {
            inner: Arc::clone(&inner),
        };

        tokio::spawn(read_loop(Arc::clone(&inner), reader));

        client
            .send_command(ServiceBusProtocolMessage {
                r#type: protocol_types::CONNECT.to_string(),
                service_id: Some(service_id),
                instance_id: Some(instance_id),
                token: Some(token),
                ..Default::default()
            })
            .await?;

        Ok(client)
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

async fn read_loop(inner: Arc<Inner>, reader: OwnedReadHalf) {
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let bytes = match reader.read_line(&mut line).await {
            Ok(size) => size,
            Err(_) => break,
        };
        if bytes == 0 {
            break;
        }

        let message: ServiceBusProtocolMessage = match serde_json::from_str(&line) {
            Ok(msg) => msg,
            Err(_) => continue,
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
