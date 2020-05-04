use crate::errors::{APIError, ErrType};
use reqwest::{header::HeaderValue, Client as HTTPClient, Method, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::path::Path;
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
    /// TODO: backoff time when retrying
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

    /// Request for a response
    ///
    /// # Arguments
    ///
    /// * `method` - A http method, such as GET/PUT/POST etc
    /// * `relative_path` - A str that holds the relative url path
    /// * `query` - A option that holds query pairs if any
    /// * `body` - A vector that holds body data if any
    async fn request<T: Serialize + ?Sized>(
        &self,
        method: Method,
        relative_path: &str,
        query: Option<&T>,
        body: Option<Vec<u8>>,
    ) -> std::result::Result<(String, reqwest::Response), reqwest::Error> {
        let url = format!("http://{}:{}", self.host, self.port);
        let url = Path::new(&url)
            .join("api")
            .join(&self.namespace)
            .join(relative_path)
            .to_str()
            .unwrap()
            .to_string();

        println!("url = {}", url);

        let mut builder = self.http_client.request(method, &url);

        if query.is_some() {
            builder = builder.query(query.unwrap())
        }

        if body.is_some() {
            builder = builder.body(body.unwrap())
        }

        let response = builder.header("X-Token", &self.token).send().await?;

        println!("got response");
        let request_id = response
            .headers()
            .get("X-Request-ID")
            .unwrap_or(&HeaderValue::from_str("").unwrap())
            .to_str()
            .unwrap()
            .to_string();

        Ok((request_id, response))
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
        if ack_job_id != "" {
            relative_path = Path::new(&relative_path)
                .join("job")
                .join(ack_job_id)
                .to_str()
                .unwrap()
                .to_string();
        }

        let query = [("ttl", ttl), ("tries", tries), ("delay", delay)];
        let (request_id, response) = self
            .request(
                Method::PUT,
                relative_path.as_str(),
                Some(&query),
                Some(data),
            )
            .await?;

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
    /// * `queue` - A string that holds the queue for the task
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

    /// Mark a job as finished, so it won't be retried by others
    ///
    /// # Arguments
    ///
    /// * queue - A string that holds the queue for the job
    /// * job_id - A string that holds the job id
    pub async fn ack(&self, queue: String, job_id: String) -> Result<()> {
        let relative_path = Path::new(&queue)
            .join("job")
            .join(job_id.clone())
            .to_str()
            .unwrap()
            .to_string();

        let ret = self
            .request::<(String, u32)>(Method::DELETE, relative_path.as_str(), None, None)
            .await;

        if ret.is_err() {
            return Err(APIError {
                err_type: ErrType::RequestErr,
                reason: ret.unwrap_err().to_string(),
                job_id: job_id,
                request_id: "".to_string(),
            })
        }

        let (request_id, response) = ret.unwrap();
        let status_code = response.status();
        if status_code != StatusCode::NO_CONTENT {
            return Err(APIError {
                err_type: ErrType::ResponseErr,
                reason: status_code.canonical_reason().unwrap().to_string(),
                job_id: job_id,
                request_id: request_id,
            })
        }

        Ok(())
    }
}
