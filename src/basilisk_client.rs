use crate::bus_client::{BusClient, ForwardRequest};
use crate::error::ClientResult;
use crate::gateway_api::{AuthInfo, GatewayApiClient, InstanceInfo, RegistrationRequest};
use crate::protocol::{ServiceBusEventEnvelope, ServiceBusForwardResponse};
use std::collections::HashMap;

/// Configuration used by `BasiliskClient::connect`.
#[derive(Debug, Clone)]
pub struct BasiliskClientConfig {
    /// Base URL for gateway registry APIs.
    pub gateway_base_url: String,
    /// Service bus host.
    pub bus_host: String,
    /// Service bus TCP port.
    pub bus_port: u16,
    /// Logical service identifier.
    pub service_id: String,
    /// Service fingerprint/version marker.
    pub fingerprint: String,
    /// Path prefixes advertised to the gateway.
    pub path_prefixes: Vec<String>,
    /// Upstream URL scheme.
    pub scheme: String,
    /// Upstream host.
    pub host: String,
    /// Upstream port.
    pub port: u16,
    /// Load-balancing weight for this instance.
    pub weight: i32,
    /// Registration auth type (for example `token`).
    pub registration_auth_type: String,
    /// Registration token/secret for gateway auth.
    pub registration_token: String,
}

/// High-level client that combines gateway registration APIs and the service-bus client.
#[derive(Clone)]
pub struct BasiliskClient {
    /// Gateway API client handle.
    pub gateway: GatewayApiClient,
    /// Service bus client handle.
    pub bus: BusClient,
    /// Current service id.
    pub service_id: String,
    /// Registered instance id.
    pub instance_id: String,
}

impl BasiliskClient {
    /// Registers the instance with the gateway and opens an authenticated bus connection.
    pub async fn connect(config: BasiliskClientConfig) -> anyhow::Result<Self> {
        let BasiliskClientConfig {
            gateway_base_url,
            bus_host,
            bus_port,
            service_id,
            fingerprint,
            path_prefixes,
            scheme,
            host,
            port,
            weight,
            registration_auth_type,
            registration_token,
        } = config;

        let gateway = GatewayApiClient::new(gateway_base_url);
        let registration = RegistrationRequest {
            service_id: service_id.clone(),
            fingerprint,
            path_prefixes,
            instance: InstanceInfo {
                instance_id: String::new(),
                scheme,
                host,
                port,
                weight,
            },
            auth: AuthInfo {
                auth_type: registration_auth_type,
                token: registration_token,
            },
        };

        let registration_response = gateway.register_instance_auto(&registration).await?;
        let instance_id = registration_response.instance_id;
        let token = registration_response.token;
        let bus = BusClient::connect(
            &bus_host,
            bus_port,
            service_id.clone(),
            instance_id.clone(),
            token,
        )
        .await?;

        Ok(Self {
            gateway,
            bus,
            service_id,
            instance_id,
        })
    }

    /// Deregisters this instance from the gateway registry.
    pub async fn deregister(&self) -> anyhow::Result<()> {
        self.gateway
            .deregister_instance(&self.service_id, &self.instance_id)
            .await
    }

    /// Subscribes this client to the provided topics.
    pub async fn subscribe(&self, topics: Vec<String>) -> ClientResult<()> {
        self.bus.subscribe(topics).await
    }

    /// Unsubscribes this client from the provided topics.
    pub async fn unsubscribe(&self, topics: Vec<String>) -> ClientResult<()> {
        self.bus.unsubscribe(topics).await
    }

    /// Publishes an event and returns the number of subscribers that received it.
    pub async fn publish(
        &self,
        topic: impl Into<String>,
        message_type: impl Into<String>,
        payload: HashMap<String, serde_json::Value>,
    ) -> ClientResult<i32> {
        self.bus.publish(topic, message_type, payload).await
    }

    /// Publishes a fully constructed event envelope.
    pub async fn publish_event(&self, event: ServiceBusEventEnvelope) -> ClientResult<i32> {
        self.bus.publish_event(event).await
    }

    /// Sends a forward request and waits for a forward response.
    pub async fn forward(
        &self,
        request: ForwardRequest,
    ) -> ClientResult<ServiceBusForwardResponse> {
        self.bus.forward(request).await
    }

    /// Registers an async event handler for a topic.
    pub async fn on_event<F, Fut>(&self, topic: impl Into<String>, handler: F) -> ClientResult<()>
    where
        F: Fn(ServiceBusEventEnvelope) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.bus.on_event(topic, handler).await
    }

    /// Registers an async request responder keyed by message type.
    pub async fn on_request<F, Fut>(
        &self,
        topic: impl Into<String>,
        responder: F,
    ) -> ClientResult<()>
    where
        F: Fn(crate::bus_client::ServiceBusRequest, crate::bus_client::RequestResponder) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = ClientResult<()>> + Send + 'static,
    {
        self.bus.on_request(topic, responder).await
    }
}
