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

## API reference

This section documents the public APIs that implement the features above.

### Top-level client (`BasiliskClient`)

Use this when you want a single workflow that registers in the gateway and then opens an authenticated bus connection.

`BasiliskClientConfig` fields:

- `gateway_base_url`: Gateway base URL (for example `http://127.0.0.1:3000`)
- `bus_host`: TCP host for the service bus
- `bus_port`: TCP port for the service bus
- `service_id`: Logical service identifier
- `fingerprint`: Service fingerprint/version marker
- `path_prefixes`: Path prefixes advertised to the gateway
- `scheme`: Upstream scheme (`http` or `https`)
- `host`: Upstream host
- `port`: Upstream port
- `weight`: Instance load-balancing weight
- `registration_auth_type`: Gateway auth type (for example `token`)
- `registration_token`: Gateway registration token/secret

`BasiliskClient` public fields:

- `gateway: GatewayApiClient`
- `bus: BusClient`
- `service_id: String`
- `instance_id: String`

`BasiliskClient` methods:

- `connect(config) -> anyhow::Result<BasiliskClient>`
  - Registers first (`/registry/register`) and then authenticates the bus connection.
- `deregister() -> anyhow::Result<()>`
  - Calls gateway deregistration for the current `service_id` + `instance_id`.
- `subscribe(topics) -> ClientResult<()>`
- `unsubscribe(topics) -> ClientResult<()>`
- `publish(topic, message_type, payload) -> ClientResult<i32>`
  - Returns subscriber count from the bus `ack` frame.
- `publish_event(event) -> ClientResult<i32>`
- `forward(request) -> ClientResult<ServiceBusForwardResponse>`
- `on_event(topic, handler) -> ClientResult<()>`
  - Registers an async event callback and auto-subscribes to that topic.
- `on_request(message_type, responder) -> ClientResult<()>`
  - Registers an async request responder keyed by message type.

### Service bus client (`BusClient`)

Use this when you need lower-level direct control over the TCP bus.

`BusClient` methods:

- `connect(host, port, service_id, instance_id, token) -> ClientResult<BusClient>`
  - First protocol frame is `connect` carrying auth credentials.
  - Frames are newline-delimited JSON (`\n` terminated).
- `subscribe(topics) -> ClientResult<()>`
- `unsubscribe(topics) -> ClientResult<()>`
- `publish(topic, message_type, payload) -> ClientResult<i32>`
- `publish_event(event) -> ClientResult<i32>`
- `forward(request) -> ClientResult<ServiceBusForwardResponse>`
- `on_event(topic, handler) -> ClientResult<()>`
- `on_request(message_type, responder) -> ClientResult<()>`

Supporting bus API types:

- `ForwardRequest`
  - `target_service_id`, `message_type`, `payload`, `timeout_ms`
- `ServiceBusRequest`
  - `event: ServiceBusEventEnvelope`
  - `reply_to() -> Option<&str>`
- `RequestResponder`
  - `respond(message_type, payload) -> ClientResult<i32>`
  - `respond_ok(payload) -> ClientResult<i32>`

### Gateway API client (`GatewayApiClient`)

Use this when you only need registry operations.

`GatewayApiClient` methods:

- `new(base_url) -> GatewayApiClient`
- `register_instance_auto(request) -> anyhow::Result<RegistrationResponse>`
  - Clears `request.instance.instance_id` and lets the registry generate it.
- `register_instance(request) -> anyhow::Result<RegistrationResponse>`
- `deregister_instance(service_id, instance_id) -> anyhow::Result<()>`

Gateway payload types:

- `RegistrationRequest`
  - `service_id`, `fingerprint`, `path_prefixes`, `instance`, `auth`
- `InstanceInfo`
  - `instance_id`, `scheme`, `host`, `port`, `weight`
- `AuthInfo`
  - `auth_type`, `token`
- `RegistrationResponse`
  - `message`, `service_id`, `instance_id`, `token`

### Protocol types

These mirror wire payloads used over the service bus.

- `ServiceBusEventEnvelope`
  - `event_id`, `emitted_at_utc`, `service_id`, `instance_id`, `topic`, `message_type`, `correlation_id`, `causation_id`, `payload`
- `ServiceBusForwardRequest`
- `ServiceBusForwardResponse`
- `ServiceBusProtocolMessage`
  - Generic frame container for `connect`, `subscribe`, `publish`, `forward`, `event`, `ack`, and `error` frames.
- `protocol_types`
  - String constants for protocol frame names.

### Error model

- `ClientResult<T> = Result<T, ClientError>`
- `ClientError`
  - `Io`, `Serde`, `Protocol { code, message }`, `MissingField`, `ChannelClosed`, `UnexpectedMessage`

All fallible bus operations return `ClientResult<T>` and preserve protocol/transport details.

## Quick usage

```rust
use rust_client::{BasiliskClient, BasiliskClientConfig, ForwardRequest, init_tracing};
use std::collections::HashMap;

async fn run() -> anyhow::Result<()> {
    init_tracing();
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

    tracing::info!(instance_id = %client.instance_id, "Connected to Basilisk");

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

    tracing::info!(message_type = %response.message_type, "Forward response received");
    Ok(())
}
```

`BasiliskClient` owns a `gateway` client for registry operations and a `bus` client for
the TCP service bus. The top-level `connect` flow automatically registers the instance,
accepts the generated instance ID returned by the registry, and then opens the bus
connection using the issued token.

## Logging

Call `init_tracing()` once during application startup to install Basilisk's
console subscriber. It reads [`RUST_LOG`](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html)
for log filtering and defaults to `INFO` when the variable is absent. If your
application has already installed a `tracing` subscriber, do not call it.

```bash
RUST_LOG=debug cargo run --example basic_service_bus
```

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
