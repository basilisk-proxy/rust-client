use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Service-bus frame type constants.
pub mod protocol_types {
    /// Connect and authenticate a socket session.
    pub const CONNECT: &str = "connect";

    /// Subscribe to one or more topics.
    pub const SUBSCRIBE: &str = "subscribe";
    /// Unsubscribe from one or more topics.
    pub const UNSUBSCRIBE: &str = "unsubscribe";
    /// Publish an event envelope.
    pub const PUBLISH: &str = "publish";
    /// Send a forward request to another service.
    pub const FORWARD: &str = "forward";
    /// Receive a forward-response payload.
    pub const FORWARD_RESPONSE: &str = "forward_response";
    /// Event pushed from the bus.
    pub const EVENT: &str = "event";
    /// Command acknowledgment.
    pub const ACK: &str = "ack";
    /// Error response frame.
    pub const ERROR: &str = "error";
}

/// Canonical event envelope transported over the Basilisk bus.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServiceBusEventEnvelope {
    /// Unique event identifier.
    #[serde(rename = "eventId")]
    pub event_id: String,
    /// UTC timestamp when the event was emitted.
    #[serde(rename = "emittedAtUtc")]
    pub emitted_at_utc: DateTime<Utc>,
    /// Origin service identifier.
    #[serde(rename = "serviceId")]
    pub service_id: String,
    /// Origin service instance identifier.
    #[serde(rename = "instanceId")]
    pub instance_id: String,
    /// Topic on which the event is routed.
    pub topic: String,
    /// Logical message type.
    #[serde(rename = "messageType")]
    pub message_type: String,
    /// Correlation identifier used to link request/response chains.
    #[serde(rename = "correlationId")]
    pub correlation_id: i64,
    /// Optional causation event id.
    #[serde(rename = "causationId")]
    pub causation_id: Option<String>,
    /// Free-form JSON payload.
    #[serde(default)]
    pub payload: HashMap<String, serde_json::Value>,
}

/// Forward request payload embedded in a `forward` frame.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServiceBusForwardRequest {
    /// Destination service id.
    #[serde(rename = "targetServiceId")]
    pub target_service_id: String,
    /// Message type to invoke on destination service.
    #[serde(rename = "messageType")]
    pub message_type: String,
    /// Free-form request payload.
    #[serde(default)]
    pub payload: HashMap<String, serde_json::Value>,
    /// Optional timeout override in milliseconds.
    #[serde(rename = "timeoutMs")]
    pub timeout_ms: Option<u64>,
}

/// Forward response payload returned by the service bus.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServiceBusForwardResponse {
    /// Response message type.
    #[serde(rename = "messageType")]
    pub message_type: String,
    /// Free-form response payload.
    #[serde(default)]
    pub payload: HashMap<String, serde_json::Value>,
}

/// Generic wire frame used by all bus protocol message types.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ServiceBusProtocolMessage {
    /// Frame type; see `protocol_types` constants.
    pub r#type: String,
    /// Service id used in connect/auth related frames.
    #[serde(rename = "serviceId")]
    pub service_id: Option<String>,
    /// Instance id used in connect/auth related frames.
    #[serde(rename = "instanceId")]
    pub instance_id: Option<String>,
    /// Authentication token used during connect.
    pub token: Option<String>,
    /// Topic list for subscribe/unsubscribe frames.
    pub topics: Option<Vec<String>>,
    /// Event payload for publish/event frames.
    pub event: Option<ServiceBusEventEnvelope>,
    /// Forward request payload.
    #[serde(rename = "forwardRequest")]
    pub forward_request: Option<ServiceBusForwardRequest>,
    /// Forward response payload.
    #[serde(rename = "forwardResponse")]
    pub forward_response: Option<ServiceBusForwardResponse>,
    /// Human-readable status or error message.
    pub message: Option<String>,
    /// Structured protocol error code.
    #[serde(rename = "errorCode")]
    pub error_code: Option<String>,
    /// Number of subscribers that received a published event.
    #[serde(rename = "subscriberCount")]
    pub subscriber_count: Option<i32>,
}
