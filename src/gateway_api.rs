use anyhow::Context;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct GatewayApiClient {
    base_url: String,
    http: reqwest::Client,
}

impl GatewayApiClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrationRequest {
    #[serde(rename = "serviceId")]
    pub service_id: String,
    pub fingerprint: String,
    #[serde(rename = "pathPrefixes")]
    pub path_prefixes: Vec<String>,
    pub instance: InstanceInfo,
    pub auth: AuthInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceInfo {
    #[serde(rename = "instanceId")]
    pub instance_id: String,
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub weight: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthInfo {
    #[serde(rename = "type")]
    pub auth_type: String,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrationResponse {
    pub message: String,
    #[serde(rename = "serviceId")]
    pub service_id: String,
    pub token: String,
}
