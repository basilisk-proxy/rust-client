use axum::{Router, extract::State, http::HeaderMap, response::IntoResponse, routing::get};
use rust_client::{
    AuthInfo, BusClient, ClientError, ForwardRequest, GatewayApiClient, InstanceInfo,
    RegistrationRequest,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tempfile::tempdir;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, sleep, timeout};

fn find_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("read local addr").port();
    drop(listener);
    port
}

async fn wait_gateway_ready(base_url: &str, gateway_child: &mut Child) {
    let client = reqwest::Client::new();
    let url = format!("{base_url}/registry/services");
    for _ in 0..1200 {
        if let Some(status) = gateway_child
            .try_wait()
            .expect("failed to inspect gateway child process")
        {
            panic!("gateway exited before readiness check completed: {status}");
        }

        if let Ok(resp) = client.get(&url).send().await
            && resp.status().is_success()
        {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("gateway did not become ready at {base_url}");
}

fn write_lua_config(file_path: &PathBuf, http_port: u16, bus_port: u16) {
    let lua = format!(
        "basilisk.server.host('127.0.0.1')\n\
         basilisk.server.port({http_port})\n\
         basilisk.gateway.strip_prefix(false)\n\
         basilisk.security.service_registration_auth('TOKEN')\n\
         basilisk.security.registration_token('secret-token')\n\
         basilisk.cache.enabled(true)\n\
         basilisk.cache.provider('memory')\n\
         basilisk.cache.key_prefix('e2e')\n\
         basilisk.cache.service_resolution_ttl_seconds(30)\n\
         basilisk.service_bus.enabled(true)\n\
         basilisk.service_bus.host('127.0.0.1')\n\
         basilisk.service_bus.port({bus_port})\n\
         basilisk.service_bus.connection_health_enabled(true)\n\
         basilisk.service_bus.monitoring_enabled(false)\n\
         basilisk.proxy.use('/api/orders', function(req, res, next)\n\
           res:forward_headers('x-from-lua', 'yes')\n\
           return next()\n\
         end)\n"
    );
    std::fs::write(file_path, lua).expect("write basilisk.lua");
}

async fn spawn_gateway(basilisk_dir: &PathBuf, lua_path: &PathBuf) -> Child {
    Command::new("cargo")
        .current_dir(basilisk_dir)
        .arg("run")
        .arg("--quiet")
        .arg("--")
        .arg(lua_path)
        .spawn()
        .expect("spawn basilisk gateway")
}

fn registration_request(
    service_id: &str,
    instance_id: &str,
    path_prefixes: Vec<String>,
    host: &str,
    port: u16,
) -> RegistrationRequest {
    RegistrationRequest {
        service_id: service_id.to_string(),
        fingerprint: format!("fp-{service_id}"),
        path_prefixes,
        instance: InstanceInfo {
            instance_id: instance_id.to_string(),
            scheme: "http".to_string(),
            host: host.to_string(),
            port,
            weight: 1,
        },
        auth: AuthInfo {
            auth_type: "token".to_string(),
            token: "secret-token".to_string(),
        },
    }
}

async fn upstream_handler(
    State(header_seen): State<Arc<AtomicBool>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if headers
        .get("x-from-lua")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == "yes")
    {
        header_seen.store(true, Ordering::SeqCst);
    }

    (
        axum::http::StatusCode::OK,
        axum::Json(serde_json::json!({"message":"upstream-ok"})),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn full_feature_client_e2e_with_gateway() {
    let http_port = find_free_port();
    let bus_port = find_free_port();
    let upstream_port = find_free_port();

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let basilisk_dir = manifest_dir
        .parent()
        .expect("rust-client should have parent directory")
        .join("basilisk");

    let temp = tempdir().expect("create temp dir");
    let lua_path = temp.path().join("basilisk.lua");
    write_lua_config(&lua_path, http_port, bus_port);

    let header_seen = Arc::new(AtomicBool::new(false));
    let upstream_state = Arc::clone(&header_seen);
    let upstream_app = Router::new()
        .route("/api/orders/check", get(upstream_handler))
        .with_state(upstream_state);
    let upstream_addr = SocketAddr::from(([127, 0, 0, 1], upstream_port));
    let upstream_listener = tokio::net::TcpListener::bind(upstream_addr)
        .await
        .expect("bind upstream listener");
    let upstream_handle = tokio::spawn(async move {
        let _ = axum::serve(upstream_listener, upstream_app).await;
    });

    let mut gateway = spawn_gateway(&basilisk_dir, &lua_path).await;
    let gateway_base = format!("http://127.0.0.1:{http_port}");
    wait_gateway_ready(&gateway_base, &mut gateway).await;

    let gateway_api = GatewayApiClient::new(gateway_base.clone());

    let orders_reg = gateway_api
        .register_instance(&registration_request(
            "orders",
            "orders-1",
            vec!["/api/orders".to_string()],
            "127.0.0.1",
            upstream_port,
        ))
        .await
        .expect("register orders");

    let billing_reg = gateway_api
        .register_instance(&registration_request(
            "billing",
            "billing-1",
            vec!["/api/billing".to_string()],
            "127.0.0.1",
            upstream_port,
        ))
        .await
        .expect("register billing");

    let orders_client = BusClient::connect(
        "127.0.0.1",
        bus_port,
        "orders",
        "orders-1",
        orders_reg.token,
    )
    .await
    .expect("connect orders bus client");

    let billing_client = BusClient::connect(
        "127.0.0.1",
        bus_port,
        "billing",
        "billing-1",
        billing_reg.token,
    )
    .await
    .expect("connect billing bus client");

    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<String>();
    orders_client
        .on_event("orders.events", move |event| {
            let tx = event_tx.clone();
            async move {
                let _ = tx.send(event.message_type);
            }
        })
        .await
        .expect("subscribe orders.events");

    let (request_seen_tx, request_seen_rx) = oneshot::channel::<()>();
    let request_seen_tx = Arc::new(tokio::sync::Mutex::new(Some(request_seen_tx)));

    orders_client
        .on_request("order.query", move |request, responder| {
            let request_seen_tx = Arc::clone(&request_seen_tx);
            async move {
                if request.reply_to().is_none() {
                    return Err(ClientError::MissingField("reply_to"));
                }

                if let Some(sender) = request_seen_tx.lock().await.take() {
                    let _ = sender.send(());
                }

                let mut payload = HashMap::new();
                payload.insert("handledBy".to_string(), serde_json::json!("orders-1"));
                payload.insert("orderId".to_string(), serde_json::json!("42"));
                responder.respond("order.query.response", payload).await?;
                Ok(())
            }
        })
        .await
        .expect("register on_request responder");

    sleep(Duration::from_millis(100)).await;

    let mut publish_payload = HashMap::new();
    publish_payload.insert("value".to_string(), serde_json::json!(1));
    let delivered = billing_client
        .publish("orders.events", "orders.created", publish_payload)
        .await
        .expect("publish event");
    assert!(
        delivered >= 1,
        "event should be delivered to at least one subscriber"
    );

    let message_type = timeout(Duration::from_secs(2), event_rx.recv())
        .await
        .expect("timed out waiting for event")
        .expect("event channel closed");
    assert_eq!(message_type, "orders.created");

    let mut forward_payload = HashMap::new();
    forward_payload.insert("orderId".to_string(), serde_json::json!("42"));
    let forward_response = billing_client
        .forward(ForwardRequest {
            target_service_id: "orders".to_string(),
            message_type: "order.query".to_string(),
            payload: forward_payload,
            timeout_ms: Some(3_000),
        })
        .await
        .expect("forward request should succeed");

    assert_eq!(forward_response.message_type, "order.query.response");
    assert_eq!(
        forward_response
            .payload
            .get("handledBy")
            .and_then(|v| v.as_str()),
        Some("orders-1")
    );

    timeout(Duration::from_secs(2), request_seen_rx)
        .await
        .expect("responder should have been invoked")
        .expect("request notification channel closed");

    let proxy_response = reqwest::get(format!("{gateway_base}/api/orders/check"))
        .await
        .expect("proxy request should complete");
    assert!(proxy_response.status().is_success());
    let proxy_json: serde_json::Value = proxy_response
        .json()
        .await
        .expect("proxy body should be json");
    assert_eq!(
        proxy_json.get("message").and_then(|v| v.as_str()),
        Some("upstream-ok")
    );

    assert!(
        header_seen.load(Ordering::SeqCst),
        "upstream should observe Lua-forwarded header"
    );

    let _ = gateway_api
        .deregister_instance("billing", "billing-1")
        .await;
    let _ = gateway_api.deregister_instance("orders", "orders-1").await;

    let _ = gateway.kill().await;
    upstream_handle.abort();
}
