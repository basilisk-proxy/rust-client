use anyhow::Context;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

/// HTTP client for Basilisk gateway registry APIs.
#[derive(Clone)]
pub struct GatewayApiClient {
    base_url: String,
    http: reqwest::Client,
}

impl GatewayApiClient {
    /// Creates a new gateway API client.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }

    /// Registers a service instance after clearing `instance.instance_id` so the gateway can generate one.
    pub async fn register_instance_auto(
        &self,
        request: &RegistrationRequest,
    ) -> anyhow::Result<RegistrationResponse> {
        let mut request = request.clone();
        request.instance.instance_id.clear();
        self.register_instance(&request).await
    }

    /// Registers a service instance at `/registry/register`.
    pub async fn register_instance(
        &self,
        request: &RegistrationRequest,
    ) -> anyhow::Result<RegistrationResponse> {
        let url = format!("{}/registry/register", self.base_url);
        let response = self
            .http
            .post(url)
            .json(request)
            .send()
            .await
            .context("failed to call /registry/register")?;

        let status = response.status();
        if status != StatusCode::OK {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<no body>".to_string());
            anyhow::bail!("register failed with status {status}: {body}");
        }

        response
            .json::<RegistrationResponse>()
            .await
            .context("failed to parse registry register response")
    }

    /// Deregisters an existing instance by service and instance id.
    pub async fn deregister_instance(
        &self,
        service_id: &str,
        instance_id: &str,
    ) -> anyhow::Result<()> {
        let url = format!(
            "{}/registry/services/{}/instances/{}",
            self.base_url, service_id, instance_id
        );
        let response = self
            .http
            .delete(url)
            .send()
            .await
            .context("failed to call deregister endpoint")?;

        let status = response.status();
        if status != StatusCode::OK {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<no body>".to_string());
            anyhow::bail!("deregister failed with status {status}: {body}");
        }

        Ok(())
    }
}

/// Request body for gateway instance registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrationRequest {
    /// Logical service identifier.
    #[serde(rename = "serviceId")]
    pub service_id: String,
    /// Service fingerprint/version marker.
    pub fingerprint: String,
    /// Path prefixes exposed by this instance.
    #[serde(rename = "pathPrefixes")]
    pub path_prefixes: Vec<String>,
    /// Network/location metadata for the running instance.
    pub instance: InstanceInfo,
    /// Registration authentication metadata.
    pub auth: AuthInfo,
}

/// Instance metadata sent to the gateway registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceInfo {
    /// Service instance id. Empty string allows server-side generation.
    #[serde(rename = "instanceId")]
    pub instance_id: String,
    /// Upstream URL scheme.
    pub scheme: String,
    /// Upstream host.
    pub host: String,
    /// Upstream port.
    pub port: u16,
    /// Load-balancing weight.
    pub weight: i32,
}

/// Registration auth metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthInfo {
    /// Auth mechanism type (for example `token`).
    #[serde(rename = "type")]
    pub auth_type: String,
    /// Shared secret/token value.
    pub token: String,
}

/// Successful response payload for instance registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrationResponse {
    /// Human-readable status message.
    pub message: String,
    /// Registered service id.
    #[serde(rename = "serviceId")]
    pub service_id: String,
    /// Registered instance id.
    #[serde(rename = "instanceId")]
    pub instance_id: String,
    /// Issued token for service-bus authentication.
    pub token: String,
}
