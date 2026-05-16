//! Basilisk Rust client for gateway registration and service-bus communication.

pub mod basilisk_client;
pub mod bus_client;
pub mod error;
pub mod gateway_api;
pub mod protocol;

/// High-level client and configuration.
pub use basilisk_client::{BasiliskClient, BasiliskClientConfig};
/// Low-level service-bus client and request/response helper types.
pub use bus_client::{
    BusClient, ForwardRequest, RequestHandler, RequestResponder, ServiceBusRequest,
};
/// Error model for bus operations.
pub use error::{ClientError, ClientResult};
/// Gateway registry API client and payload types.
pub use gateway_api::{
    AuthInfo, GatewayApiClient, InstanceInfo, RegistrationRequest, RegistrationResponse,
};
/// Service-bus protocol frame and payload types.
pub use protocol::{
    ServiceBusEventEnvelope, ServiceBusForwardResponse, ServiceBusProtocolMessage, protocol_types,
};
