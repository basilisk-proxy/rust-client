use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod protocol_types {
    pub const CONNECT: &str = "connect";
    pub const AUTHENTICATE: &str = "authenticate";
    pub const SUBSCRIBE: &str = "subscribe";
    pub const UNSUBSCRIBE: &str = "unsubscribe";
    pub const PUBLISH: &str = "publish";
    pub const FORWARD: &str = "forward";
    pub const FORWARD_RESPONSE: &str = "forward_response";
    pub const EVENT: &str = "event";
    pub const ACK: &str = "ack";
    pub const ERROR: &str = "error";
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServiceBusEventEnvelope {
    #[serde(rename = "eventId")]
    pub event_id: String,
    #[serde(rename = "emittedAtUtc")]
    pub emitted_at_utc: DateTime<Utc>,
    #[serde(rename = "serviceId")]
    pub service_id: String,
    #[serde(rename = "instanceId")]
    pub instance_id: String,
    pub topic: String,
    #[serde(rename = "messageType")]
    pub message_type: String,
    #[serde(rename = "correlationId")]
    pub correlation_id: i64,
    #[serde(rename = "causationId")]
    pub causation_id: Option<String>,
    #[serde(default)]
    pub payload: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServiceBusForwardRequest {
    #[serde(rename = "targetServiceId")]
    pub target_service_id: String,
    #[serde(rename = "messageType")]
    pub message_type: String,
    #[serde(default)]
    pub payload: HashMap<String, serde_json::Value>,
    #[serde(rename = "timeoutMs")]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServiceBusForwardResponse {
    #[serde(rename = "messageType")]
    pub message_type: String,
    #[serde(default)]
    pub payload: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ServiceBusProtocolMessage {
    pub r#type: String,
    #[serde(rename = "serviceId")]
    pub service_id: Option<String>,
    #[serde(rename = "instanceId")]
    pub instance_id: Option<String>,
    pub token: Option<String>,
    pub topics: Option<Vec<String>>,
    pub event: Option<ServiceBusEventEnvelope>,
    #[serde(rename = "forwardRequest")]
    pub forward_request: Option<ServiceBusForwardRequest>,
    #[serde(rename = "forwardResponse")]
    pub forward_response: Option<ServiceBusForwardResponse>,
    pub message: Option<String>,
    #[serde(rename = "errorCode")]
    pub error_code: Option<String>,
    #[serde(rename = "subscriberCount")]
    pub subscriber_count: Option<i32>,
}
