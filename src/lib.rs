//! Basilisk Rust client for gateway registration and service-bus communication.

pub mod basilisk_client;
pub mod bus_client;
pub mod error;
pub mod gateway_api;
pub mod protocol;

/// Installs a `tracing` subscriber for Basilisk diagnostics.
///
/// The subscriber uses the `RUST_LOG` environment variable when it is set and
/// defaults to the `INFO` level otherwise. Calling this is safe when the host
/// application has already installed a subscriber; its initialization attempt
/// is ignored in that case.
pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let (env_filter, filter_source) = match EnvFilter::try_from_default_env() {
        Ok(filter) => (filter, "RUST_LOG"),
        Err(_) => (EnvFilter::new("INFO"), "DEFAULT"),
    };

    let _ = tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .try_init();
    tracing::info!(filter_source, "Log verbosity configured");
}

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
