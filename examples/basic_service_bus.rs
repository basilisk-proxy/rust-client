use rust_client::{BusClient, ForwardRequest};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Replace with a real token returned from /registry/register.
    let client = BusClient::connect("127.0.0.1", 5090, "orders", "orders-1", "replace-me").await?;

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
