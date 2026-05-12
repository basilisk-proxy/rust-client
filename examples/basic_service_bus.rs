use rust_client::{BasiliskClient, BasiliskClientConfig, ForwardRequest};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = BasiliskClient::connect(BasiliskClientConfig {
        gateway_base_url: "http://127.0.0.1:3000".to_string(),
        bus_host: "127.0.0.1".to_string(),
        bus_port: 5090,
        service_id: "orders".to_string(),
        fingerprint: "orders-v1".to_string(),
        path_prefixes: vec!["/api/orders".to_string()],
        scheme: "http".to_string(),
        host: "127.0.0.1".to_string(),
        port: 7001,
        weight: 1,
        registration_auth_type: "token".to_string(),
        registration_token: "replace-me".to_string(),
    })
    .await?;

    client
        .on_request("order.query", |_request, responder| async move {
            let mut payload = HashMap::new();
            payload.insert("status".to_string(), serde_json::json!("ok"));
            responder.respond_ok(payload).await?;
            Ok(())
        })
        .await?;

    let response = client
        .forward(ForwardRequest {
            target_service_id: "orders".to_string(),
            message_type: "order.query".to_string(),
            payload: HashMap::new(),
            timeout_ms: Some(3_000),
        })
        .await?;

    println!("Forward response type: {}", response.message_type);
    Ok(())
}
