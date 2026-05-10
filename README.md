# rust-client

Rust client library for Basilisk gateway + service bus.

## Features

- Connect/authenticate to Basilisk service bus over TCP (newline-delimited JSON protocol)
- Subscribe/unsubscribe to topics and receive events
- Publish events with structured payloads
- Send forward requests and wait for forward responses
- Register request responders with `bus_client.on_request(message_type, responder)`
- Call gateway registry APIs to register/deregister service instances

## Quick usage

```rust
use rust_client::{BusClient, ForwardRequest};
use std::collections::HashMap;

async fn run() -> anyhow::Result<()> {
let client = BusClient::connect("127.0.0.1", 5090, "orders", "orders-1", "instance-token").await?;

client.on_request("order.query", |request, responder| async move {
    let mut payload = HashMap::new();
    payload.insert("orderId".to_string(), serde_json::json!("42"));
    responder.respond_ok(payload).await?;
    Ok(())
}).await?;

let response = client.forward(ForwardRequest {
    target_service_id: "orders".to_string(),
    message_type: "order.query".to_string(),
    payload: HashMap::new(),
    timeout_ms: Some(2_000),
}).await?;

println!("{}", response.message_type);
 Ok(())
}
```

## End-to-end test

The integration test starts:

- a temporary Basilisk gateway instance,
- an upstream HTTP service,
- two service bus clients (`orders` and `billing`).

It validates registry registration, proxy forwarding + Lua middleware header forwarding, publish/subscribe, and forward-request responder behavior.

Run:

```bash
cargo test -- --nocapture
```
