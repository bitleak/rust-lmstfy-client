use crate::errors::{APIError, ErrType};
use reqwest::{header::HeaderValue, Client as HTTPClient, StatusCode};
use serde::Deserialize;
use std::path::Path;

const MAX_READ_TIMEOUT: u32 = 600;
const MAX_BATCH_CONSUME_SIZE: u32 = 100;

type Result<T> = std::result::Result<T, APIError>;

/// The client for lmstfy
pub struct Client {
    pub namespace: String,
    pub token: String,
    pub host: String,
    pub port: u32,
    /// Retry when `publish` failed
    pub retry: u32,
    /// Unit: millisecond
    pub back_off: u32,
    /// http client
    pub http_client: HTTPClient,
}

#[derive(Deserialize)]
pub struct PublishResponse {
    job_id: String,
}

/// The client itself
impl Client {
    pub fn new(
        namespace: &str,
        token: &str,
        host: &str,
        port: u32,
        retry: u32,
        back_off: u32,
    ) -> Self {
        Client {
            namespace: namespace.to_string(),
            token: token.to_string(),
            host: host.to_string(),
            port: port,
            retry: retry,
            back_off: back_off,
            http_client: HTTPClient::new(),
        }
    }

    /// Innner function to publish message
    async fn do_publish(
        &self,
        queue: String,
        ack_job_id: String,
        data: Vec<u8>,
        ttl: u32,
        tries: u32,
        delay: u32,
    ) -> std::result::Result<(String, String, StatusCode), reqwest::Error> {
        let mut relative_path = queue.clone();
        if ack_job_id == "" {
            relative_path = Path::new(&relative_path)
                .join("job")
                .join(ack_job_id)
                .to_str()
                .unwrap()
                .to_string();
        }

        let url = format!("http://{}:{}", self.host, self.port);
        let url = Path::new(&url)
            .join("api")
            .join(&self.namespace)
            .join(relative_path)
            .to_str()
            .unwrap()
            .to_string();

        println!("url = {}", url);

        let response = self
            .http_client
            .put(&url)
            .query(&[("ttl", ttl), ("tries", tries), ("delay", delay)])
            .header("X-Token", &self.token)
            .body(data)
            .send()
            .await?;

        println!("got response");
        let request_id = response
            .headers()
            .get("X-Request-ID")
            .unwrap_or(&HeaderValue::from_str("").unwrap())
            .to_str()
            .unwrap()
            .to_string();

        let status_code = response.status();
        let response = response.json::<PublishResponse>().await?;

        Ok((response.job_id, request_id, status_code))
    }
}

/// The API implementation for lmstfy
impl Client {
    pub async fn publish(
        &self,
        queue: String,
        ack_job_id: String,
        data: Vec<u8>,
        ttl: u32,
        tries: u32,
        delay: u32,
    ) -> Result<(String, String)> {
        let ret = self
            .do_publish(queue, ack_job_id, data, ttl, tries, delay)
            .await;

        if ret.is_err() {
            return Err(APIError {
                err_type: ErrType::RequestErr,
                reason: ret.unwrap_err().to_string(),
                job_id: "".to_string(),
                request_id: "".to_string(),
            });
        }

        let (job_id, request_id, status_code) = ret.unwrap();
        if status_code.is_success() {
            return Ok((job_id, request_id));
        }

        return Err(APIError {
            err_type: ErrType::ResponseErr,
            reason: status_code.canonical_reason().unwrap().to_string(),
            job_id: job_id,
            request_id: request_id,
        });
    }
}
