# rust-client

Rust client library for Milestone Basilisk gateway and service bus.

## Features

- Register a service instance, receive a generated instance ID, and establish an authenticated service bus connection in one workflow
- Subscribe/unsubscribe to topics and receive events
- Publish events with structured payloads
- Send forward requests and wait for forward responses
- Register request responders with `client.on_request(message_type, responder)`
- Call gateway registry APIs to register/deregister service instances
- Use `BasiliskClient` as the top-level client with `gateway` and `bus` handles

## Quick usage

```rust
use rust_client::{BasiliskClient, BasiliskClientConfig, ForwardRequest};
use std::collections::HashMap;

async fn run() -> anyhow::Result<()> {
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

    println!("instance id: {}", client.instance_id);

    client
        .on_request("order.query", |_request, responder| async move {
            let mut payload = HashMap::new();
            payload.insert("orderId".to_string(), serde_json::json!("42"));
            responder.respond_ok(payload).await?;
            Ok(())
        })
        .await?;

    let response = client
        .forward(ForwardRequest {
            target_service_id: "orders".to_string(),
            message_type: "order.query".to_string(),
            payload: HashMap::new(),
            timeout_ms: Some(2_000),
        })
        .await?;

    println!("{}", response.message_type);
    Ok(())
}
```

`BasiliskClient` owns a `gateway` client for registry operations and a `bus` client for
the TCP service bus. The top-level `connect` flow automatically registers the instance,
accepts the generated instance ID returned by the registry, and then opens the bus
connection using the issued token.

## Notes on low-level clients

- `BusClient::connect(host, port, service_id, instance_id, token)` now authenticates during
  the `connect` handshake (no separate authenticate command is required for normal clients).
- `GatewayApiClient::register_instance(...)` responses include both `instance_id` and `token`.
- `GatewayApiClient::register_instance_auto(...)` sends an empty instance ID so the registry
  generates a cryptographically strong instance identity.

## End-to-end test

The integration test starts:

- a temporary Basilisk gateway instance,
- an upstream HTTP service,
- two connected Basilisk clients (`orders` and `billing`).

It validates registry registration, proxy forwarding and Lua middleware header forwarding, publish/subscribe, and forward-request responder behavior.

Run:

```bash
cargo test -- --nocapture
```
