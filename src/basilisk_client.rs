use crate::bus_client::{BusClient, ForwardRequest};
use crate::error::ClientResult;
use crate::gateway_api::{AuthInfo, GatewayApiClient, InstanceInfo, RegistrationRequest};
use crate::protocol::{ServiceBusEventEnvelope, ServiceBusForwardResponse};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct BasiliskClientConfig {
    pub gateway_base_url: String,
    pub bus_host: String,
    pub bus_port: u16,
    pub service_id: String,
    pub fingerprint: String,
    pub path_prefixes: Vec<String>,
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub weight: i32,
    pub registration_auth_type: String,
    pub registration_token: String,
}

#[derive(Clone)]
pub struct BasiliskClient {
    pub gateway: GatewayApiClient,
    pub bus: BusClient,
    pub service_id: String,
    pub instance_id: String,
}

impl BasiliskClient {
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

    pub async fn deregister(&self) -> anyhow::Result<()> {
        self.gateway
            .deregister_instance(&self.service_id, &self.instance_id)
            .await
    }

    pub async fn subscribe(&self, topics: Vec<String>) -> ClientResult<()> {
        self.bus.subscribe(topics).await
    }

    pub async fn unsubscribe(&self, topics: Vec<String>) -> ClientResult<()> {
        self.bus.unsubscribe(topics).await
    }

    pub async fn publish(
        &self,
        topic: impl Into<String>,
        message_type: impl Into<String>,
        payload: HashMap<String, serde_json::Value>,
    ) -> ClientResult<i32> {
        self.bus.publish(topic, message_type, payload).await
    }

    pub async fn publish_event(&self, event: ServiceBusEventEnvelope) -> ClientResult<i32> {
        self.bus.publish_event(event).await
    }

    pub async fn forward(
        &self,
        request: ForwardRequest,
    ) -> ClientResult<ServiceBusForwardResponse> {
        self.bus.forward(request).await
    }

    pub async fn on_event<F, Fut>(&self, topic: impl Into<String>, handler: F) -> ClientResult<()>
    where
        F: Fn(ServiceBusEventEnvelope) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        self.bus.on_event(topic, handler).await
    }

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
        Fut: std::future::Future<Output = ClientResult<()>> + Send + 'static,
    {
        self.bus.on_request(topic, responder).await
    }
}
