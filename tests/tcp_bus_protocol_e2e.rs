use chrono::Utc;
use rust_client::{
    AuthInfo, GatewayApiClient, InstanceInfo, RegistrationRequest, ServiceBusEventEnvelope,
    ServiceBusProtocolMessage, protocol_types,
};
use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::process::{Child, Command};
use tokio::time::{Duration, sleep};

fn find_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("read local addr").port();
    drop(listener);
    port
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
         basilisk.cache.key_prefix('tcp-test')\n\
         basilisk.cache.service_resolution_ttl_seconds(30)\n\
         basilisk.service_bus.enabled(true)\n\
         basilisk.service_bus.host('127.0.0.1')\n\
         basilisk.service_bus.port({bus_port})\n\
         basilisk.service_bus.connection_health_enabled(true)\n\
         basilisk.service_bus.monitoring_enabled(false)\n"
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

fn registration_request(service_id: &str, instance_id: &str) -> RegistrationRequest {
    RegistrationRequest {
        service_id: service_id.to_string(),
        fingerprint: format!("fp-{service_id}"),
        path_prefixes: vec![format!("/api/{service_id}")],
        instance: InstanceInfo {
            instance_id: instance_id.to_string(),
            scheme: "http".to_string(),
            host: "127.0.0.1".to_string(),
            port: 65530,
            weight: 1,
        },
        auth: AuthInfo {
            auth_type: "token".to_string(),
            token: "secret-token".to_string(),
        },
    }
}

async fn write_msg(writer: &mut OwnedWriteHalf, msg: &ServiceBusProtocolMessage) {
    let mut wire = serde_json::to_string(msg).expect("serialize protocol msg");
    wire.push('\n');
    writer
        .write_all(wire.as_bytes())
        .await
        .expect("write protocol message");
    writer.flush().await.expect("flush protocol message");
}

async fn read_msg(reader: &mut BufReader<OwnedReadHalf>) -> ServiceBusProtocolMessage {
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .expect("read protocol message line");
    serde_json::from_str(&line).expect("parse protocol message")
}

async fn connect_and_auth(
    bus_port: u16,
    service_id: &str,
    instance_id: &str,
    token: &str,
) -> (BufReader<OwnedReadHalf>, OwnedWriteHalf) {
    let stream = TcpStream::connect(("127.0.0.1", bus_port))
        .await
        .expect("connect to service bus");
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    write_msg(
        &mut write_half,
        &ServiceBusProtocolMessage {
            r#type: protocol_types::CONNECT.to_string(),
            service_id: Some(service_id.to_string()),
            instance_id: Some(instance_id.to_string()),
            ..Default::default()
        },
    )
    .await;
    let connect_ack = read_msg(&mut reader).await;
    assert_eq!(connect_ack.r#type, protocol_types::ACK);

    write_msg(
        &mut write_half,
        &ServiceBusProtocolMessage {
            r#type: protocol_types::AUTHENTICATE.to_string(),
            token: Some(token.to_string()),
            ..Default::default()
        },
    )
    .await;
    let auth_ack = read_msg(&mut reader).await;
    assert_eq!(auth_ack.r#type, protocol_types::ACK);

    (reader, write_half)
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_bus_protocol_supports_publish_subscribe_and_forward_request_response() {
    let http_port = find_free_port();
    let bus_port = find_free_port();

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let basilisk_dir = manifest_dir
        .parent()
        .expect("rust-client should have parent directory")
        .join("basilisk");

    let temp = tempdir().expect("create temp dir");
    let lua_path = temp.path().join("basilisk.lua");
    write_lua_config(&lua_path, http_port, bus_port);

    let mut gateway = spawn_gateway(&basilisk_dir, &lua_path).await;
    let gateway_base = format!("http://127.0.0.1:{http_port}");
    wait_gateway_ready(&gateway_base, &mut gateway).await;

    let gateway_api = GatewayApiClient::new(gateway_base);

    let orders_reg = gateway_api
        .register_instance(&registration_request("orders", "orders-1"))
        .await
        .expect("register orders service");

    let billing_reg = gateway_api
        .register_instance(&registration_request("billing", "billing-1"))
        .await
        .expect("register billing service");

    let (mut orders_reader, mut orders_writer) =
        connect_and_auth(bus_port, "orders", "orders-1", &orders_reg.token).await;
    let (mut billing_reader, mut billing_writer) =
        connect_and_auth(bus_port, "billing", "billing-1", &billing_reg.token).await;

    write_msg(
        &mut orders_writer,
        &ServiceBusProtocolMessage {
            r#type: protocol_types::SUBSCRIBE.to_string(),
            topics: Some(vec![
                "orders.events".to_string(),
                "service-orders".to_string(),
            ]),
            ..Default::default()
        },
    )
    .await;
    let sub_ack = read_msg(&mut orders_reader).await;
    assert_eq!(sub_ack.r#type, protocol_types::ACK);

    let publish_payload = HashMap::from([(String::from("value"), serde_json::json!(1))]);
    write_msg(
        &mut billing_writer,
        &ServiceBusProtocolMessage {
            r#type: protocol_types::PUBLISH.to_string(),
            event: Some(ServiceBusEventEnvelope {
                event_id: String::new(),
                emitted_at_utc: Utc::now(),
                service_id: String::new(),
                instance_id: String::new(),
                topic: "orders.events".to_string(),
                message_type: "orders.created".to_string(),
                correlation_id: 0,
                causation_id: None,
                payload: publish_payload,
            }),
            ..Default::default()
        },
    )
    .await;

    let publish_ack = read_msg(&mut billing_reader).await;
    assert_eq!(publish_ack.r#type, protocol_types::ACK);
    assert!(publish_ack.subscriber_count.unwrap_or_default() >= 1);

    let published_event = read_msg(&mut orders_reader).await;
    assert_eq!(published_event.r#type, protocol_types::EVENT);
    let published_envelope = published_event.event.expect("published event payload");
    assert_eq!(published_envelope.topic, "orders.events");
    assert_eq!(published_envelope.message_type, "orders.created");

    write_msg(
        &mut billing_writer,
        &ServiceBusProtocolMessage {
            r#type: protocol_types::FORWARD.to_string(),
            forward_request: Some(rust_client::protocol::ServiceBusForwardRequest {
                target_service_id: "orders".to_string(),
                message_type: "order.query".to_string(),
                payload: HashMap::from([(String::from("orderId"), serde_json::json!("42"))]),
                timeout_ms: Some(3_000),
            }),
            ..Default::default()
        },
    )
    .await;

    let forward_request_msg = read_msg(&mut orders_reader).await;
    assert_eq!(forward_request_msg.r#type, protocol_types::EVENT);
    let forward_event = forward_request_msg.event.expect("forwarded event payload");
    assert_eq!(forward_event.topic, "service-orders");
    assert_eq!(forward_event.message_type, "order.query");

    let reply_to = forward_event
        .payload
        .get("reply_to")
        .and_then(|value| value.as_str())
        .expect("forwarded request must include reply_to")
        .to_string();

    write_msg(
        &mut orders_writer,
        &ServiceBusProtocolMessage {
            r#type: protocol_types::PUBLISH.to_string(),
            event: Some(ServiceBusEventEnvelope {
                event_id: String::new(),
                emitted_at_utc: Utc::now(),
                service_id: String::new(),
                instance_id: String::new(),
                topic: reply_to,
                message_type: "order.query.response".to_string(),
                correlation_id: forward_event.correlation_id,
                causation_id: Some(forward_event.event_id.clone()),
                payload: HashMap::from([
                    (String::from("handledBy"), serde_json::json!("orders-1")),
                    (String::from("orderId"), serde_json::json!("42")),
                ]),
            }),
            ..Default::default()
        },
    )
    .await;

    let response_publish_ack = read_msg(&mut orders_reader).await;
    assert_eq!(response_publish_ack.r#type, protocol_types::ACK);

    let forward_response_msg = read_msg(&mut billing_reader).await;
    assert_eq!(
        forward_response_msg.r#type,
        protocol_types::FORWARD_RESPONSE
    );
    let forward_response = forward_response_msg
        .forward_response
        .expect("forwardResponse payload");
    assert_eq!(forward_response.message_type, "order.query.response");
    assert_eq!(
        forward_response
            .payload
            .get("handledBy")
            .and_then(|value| value.as_str()),
        Some("orders-1")
    );

    let _ = gateway_api
        .deregister_instance("billing", "billing-1")
        .await;
    let _ = gateway_api.deregister_instance("orders", "orders-1").await;

    let _ = gateway.kill().await;
}
