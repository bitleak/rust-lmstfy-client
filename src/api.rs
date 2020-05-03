use std::path::Path;
use serde::Deserialize;
use reqwest::{header::HeaderValue, Client as HTTPClient, StatusCode};
use crate::errors::{APIError, ErrType};
/// Max read timeout value in second
const MAX_READ_TIMEOUT: u32 = 600;
/// Max batch consume size
const MAX_BATCH_CONSUME_SIZE: u32 = 100;

type Result<T> = std::result::Result<T, APIError>;

/// The client for lmstfy
pub struct Client {
    /// The namespace for the client
    pub namespace: String,
    /// The access token of api
    pub token: String,
    /// The target host for the server
    pub host: String,
    /// The target port for the server
    pub port: u32,
    /// Retry when `publish` failed
    pub retry: u32,
    /// backoff time when retrying
    pub backoff: u32,
    /// http client which is used to communicate with server
    pub http_client: HTTPClient,
}

/// The normal response for publishing tasks 
#[derive(Deserialize)]
pub struct PublishResponse {
    /// The job id corresponding to the published task
    job_id: String,
}

/// The client implementation
impl Client {
    /// Returns a client with the given parameters
    /// 
    /// # Arguments
    /// 
    /// * `namespace` - A str that holds the namespace of the client
    /// * `token` - A str that holds the token for api request
    /// * `host` - A str that holds the target server
    /// * `port` - A u32 value that holds the target port
    /// * `retry` - A u32 value that holds the retry count
    /// * `backoff` - A u32 value that holds the backoff value
    pub fn new(
        namespace: &str,
        token: &str,
        host: &str,
        port: u32,
        retry: u32,
        backoff: u32,
    ) -> Self {
        Client {
            namespace: namespace.to_string(),
            token: token.to_string(),
            host: host.to_string(),
            port: port,
            retry: retry,
            backoff: backoff,
            http_client: HTTPClient::new(),
        }
    }

    /// Innner function to publish task
    /// 
    /// # Arguments
    /// 
    /// * `queue` - A string that holds the queue for the task
    /// * `ack_job_id` - A string that holds the job id about to acknowledged
    /// * `data` - A vector of byte that holds the content of task
    /// * `ttl` - A u32 value that holds the time-to-live value
    /// * `tries` - A u32 value that holds the maximize retry count
    /// * `delay` - A u32 value that holds the delay value in second
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
    // Publish task to the server
    /// 
    /// # Arguments
    /// 
    /// * `queue` - A String that holds the queue for the task
    /// * `ack_job_id` - A string that holds the job id about to acknowledged
    /// * `data` - A vector of byte that holds the content of task
    /// * `ttl` - A u32 value that holds the time-to-live value
    /// * `tries` - A u32 value that holds the maximize retry count
    /// * `delay` - A u32 value that holds the delay value in second
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
        if !status_code.is_success() {
            return Err(APIError {
                err_type: ErrType::ResponseErr,
                reason: status_code.canonical_reason().unwrap().to_string(),
                job_id: job_id,
                request_id: request_id,
            }); 
        }

        return Ok((job_id, request_id));
    }
}
