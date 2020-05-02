
use crate::errors::{APIError, ErrType};
use reqwest::Client as HTTPClient;
use std::path::Path;
use serde::Deserialize;

const MAX_READ_TIMEOUT:u32 = 600;
const MAX_BATCH_CONSUME_SIZE:u32 = 100;

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
    job_id: String
}

/// The client itself
impl Client {
    pub fn new(namespace: &str, token: &str, host: &str, port: u32, retry: u32, back_off: u32) -> Self {
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

    /// Inner function to construct url and query string
    
    /// Innner function to publish message
    async fn do_publish(&self, queue: String, ack_job_id: String, data: Vec<u8>, ttl: u32, tries: u32, delay: u32) -> std::result::Result<String, reqwest::Error> {
        let mut relative_path = queue.clone();
        if ack_job_id == "" {
            relative_path = Path::new(&relative_path)
                .join("job")
                .join(ack_job_id)
                .to_str()
                .unwrap()
                .to_string();

            println!("relative_path = {}", relative_path);
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
        
        let response = self.http_client.put(&url)
            .query(&("ttl", ttl))
            .query(&("tries", tries))
            .query(&("delay", delay))
            .header("X-Token", &self.token)
            .body(data)
            .send()
            .await?
            .json::<PublishResponse>()
            .await?;

        Ok(response.job_id)
    }
}

/// The API implementation for lmstfy 
impl Client {
    async fn publish(&self, queue: String, ack_job_id: String, data: Vec<u8>, ttl: u32, tries: u32, delay: u32) -> Result<String> {
        let ret = self.do_publish(queue, ack_job_id, data, ttl, tries, delay)
            .await;
        if ret.is_err() {
            return Err(APIError {
                err_type: ErrType::RequestErr,
                reason: ret.map_err(|e|e.to_string()).unwrap(),
                // TODO: 
                job_id: "xxx".to_string(),
                request_id: "yyy".to_string(),
            })
        }

       Ok(ret.unwrap()) 
    }
}