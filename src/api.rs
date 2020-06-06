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

#[derive(Deserialize, Debug)]
pub struct Job {
    /// The namespace of the job
    pub namespace: String,
    /// The queue which holds the job
    pub queue: String,
    /// The data holded by the job, note that data is base64 encoded
    pub data: String,
    /// The id of the job
    pub job_id: String,
    /// The time-to-live of the job
    pub ttl: u64,
    /// The elapsed value in ms
    pub elapsed_ms: u64,
}

#[derive(Deserialize)]
pub struct QueueInfo {
    /// The namespace of the queue
    pub namespace: String,
    /// The queue name
    pub queue: String,
    /// The size of the queue
    pub size: u32,
}

#[derive(Deserialize)]
pub struct DeadLetterInfo {
    /// The namespace of the queue
    pub namespace: String,
    /// The queue name
    pub queue: String,
    /// The size of the dead letters
    pub deadletter_size: u32,
    /// The header of the letters
    pub deadletter_head: String,
}

#[derive(Deserialize)]
pub struct RespawnResult {
    /// The count of respawned dead letters
    pub count: u32,
}

#[derive(Deserialize)]
pub struct ResponseError {
    /// The error info returned from server
    pub error: String,
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
    ) -> std::result::Result<(String, reqwest::Response), reqwest::Error> {
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
        self.request(
            Method::PUT,
            relative_path.as_str(),
            Some(&query),
            Some(data),
        )
        .await
    }

    /// Inner function to consume a job
    ///
    /// # Arguments
    ///
    /// * `queues` - A string vector that holds the queues for consuming
    ///
    /// * `ttr` - A u32 value that holds the time-to-run value in second.
    /// If the job is not finished before the `ttr` expires, the job will be
    /// released for consuming again if the `(tries-1) > 0`
    ///
    /// * `timeout` - A u32 value that holds the max waiting time for long polling
    /// If it's zero, this method will return immediately with or without a job;
    /// if it's positive, this method would polling for new job until timeout.
    ///
    /// * `count` - A u32 value that holds the job count of this consume.
    /// If it's zero or over 100, this method will return an error.
    /// If it's positive, this method would return some jobs, and it's count is between 0 and count.
    async fn do_consume(
        &self,
        queues: Vec<String>,
        ttr: u32,
        timeout: u32,
        count: u32,
    ) -> Result<Vec<Job>> {
        if ttr <= 0 {
            return Err(APIError {
                err_type: ErrType::RequestErr,
                reason: "ttr should be > 0".to_string(),
                ..APIError::default()
            });
        }

        if timeout >= MAX_READ_TIMEOUT {
            return Err(APIError {
                err_type: ErrType::RequestErr,
                reason: format!("timeout should be >= 0 && < {}", MAX_READ_TIMEOUT),
                ..APIError::default()
            });
        }

        if count <= 0 || count > MAX_BATCH_CONSUME_SIZE {
            return Err(APIError {
                err_type: ErrType::RequestErr,
                reason: format!("count should be > 0 && < {}", MAX_BATCH_CONSUME_SIZE),
                ..APIError::default()
            });
        }

        let relative_path = queues.join(",");
        // FIXME: count can be omitted when consuming a single job
        let query = [("ttr", ttr), ("timeout", timeout), ("count", count)];

        let ret = self
            .request(Method::GET, relative_path.as_str(), Some(&query), None)
            .await;

        if ret.is_err() {
            return Err(APIError {
                err_type: ErrType::RequestErr,
                reason: ret.unwrap_err().to_string(),
                ..APIError::default()
            });
        }

        let (request_id, response) = ret.unwrap();
        let status_code = response.status();
        if status_code == StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }

        if status_code != StatusCode::OK {
            return Err(APIError {
                err_type: ErrType::ResponseErr,
                reason: self.parse_response_error(response).await,
                request_id: request_id,
                ..APIError::default()
            });
        }

        if count == 1 {
            return response
                .json::<Job>()
                .await
                .map(|job| vec![job])
                .map_err(|e| APIError {
                    err_type: ErrType::ResponseErr,
                    reason: e.to_string(),
                    request_id: request_id,
                    ..APIError::default()
                });
        }
        // FIXME: deserialize vector of string
        response.json::<Vec<Job>>().await.map_err(|e| APIError {
            err_type: ErrType::ResponseErr,
            reason: e.to_string(),
            request_id: request_id,
            ..APIError::default()
        })
    }

    /// Inner function to peek a job
    ///
    /// # Arguments
    ///
    /// * queue - A string that holds the queue name
    /// * job_id - An option of string that holds the job id
    async fn do_peek_job(&self, queue: String, job_id: Option<String>) -> Result<Option<Job>> {
        let path = Path::new(&queue);

        let path_buf = if job_id.is_none() {
            path.join("peek")
        } else {
            path.join("job").join(&job_id.unwrap())
        };

        let relative_path = path_buf.to_str().unwrap().to_string();

        let ret = self
            .request::<(String, u32)>(Method::GET, relative_path.as_str(), None, None)
            .await;

        if ret.is_err() {
            return Err(APIError {
                err_type: ErrType::RequestErr,
                reason: ret.unwrap_err().to_string(),
                ..APIError::default()
            });
        }

        let (request_id, response) = ret.unwrap();
        let status_code = response.status();
        match status_code {
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::OK => response
                .json::<Job>()
                .await
                .map(|job| Some(job))
                .map_err(|e| APIError {
                    err_type: ErrType::ResponseErr,
                    reason: e.to_string(),
                    request_id: request_id,
                    ..APIError::default()
                }),
            _ => Err(APIError {
                err_type: ErrType::ResponseErr,
                reason: self.parse_response_error(response).await,
                job_id: "".to_string(),
                request_id: request_id,
            }),
        }
    }

    /// Inner function to parse error info from response
    ///
    /// # Arguments
    ///
    /// * `response` - A response that holds the request result
    async fn parse_response_error(&self, response: Response) -> String {
        let status_code = response.status();
        match response.json::<ResponseError>().await {
            Ok(re) => re.error,
            Err(e) => format!("[{}]{}", status_code, e),
        }
    }
}

/// The API implementation for lmstfy
impl Client {
    /// Publish task to the server
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
                ..APIError::default()
            });
        }

        let (request_id, response) = ret.unwrap();
        let status_code = response.status();

        match status_code {
            StatusCode::CREATED => response
                .json::<PublishResponse>()
                .await
                .map(|pr| (pr.job_id, request_id.clone()))
                .map_err(|e| APIError {
                    err_type: ErrType::ResponseErr,
                    reason: e.to_string(),
                    request_id: request_id,
                    ..APIError::default()
                }),
            _ => Err(APIError {
                err_type: ErrType::ResponseErr,
                reason: self.parse_response_error(response).await,
                request_id: request_id,
                ..APIError::default()
            }),
        }
    }

    /// Consume a job, consuming will decrease the job's retry count by 1 firstly
    ///
    /// # Arguments
    ///
    /// * `queue` - A string that holds the queue for consuming
    ///
    /// * `ttr` - A u32 value that holds the time-to-run value in second.
    /// If the job is not finished before the `ttr` expires, the job will be
    /// released for consuming again if the `(tries-1) > 0`
    ///
    /// * `timeout` - A u32 value that holds the max waiting time for long polling
    /// If it's zero, this method will return immediately with or without a job;
    /// if it's positive, this method would polling for new job until timeout.
    pub async fn consume(&self, queue: String, ttr: u32, timeout: u32) -> Result<Vec<Job>> {
        self.do_consume(vec![queue], ttr, timeout, 1).await
    }

    /// Consume a batch of jobs
    ///
    /// # Arguments
    ///
    /// * `queue` - A string that holds the queue for consuming
    ///
    /// * `ttr` - A u32 value that holds the time-to-run value in second.
    /// If the job is not finished before the `ttr` expires, the job will be
    /// released for consuming again if the `(tries-1) > 0`
    ///
    /// * `timeout` - A u32 value that holds the max waiting time for long polling
    /// If it's zero, this method will return immediately with or without a job;
    /// if it's positive, this method would polling for new job until timeout.
    ///
    /// * `count` - A u32 value that holds the count of wanted jobs
    pub async fn batch_consume(
        &self,
        queue: String,
        ttr: u32,
        timeout: u32,
        count: u32,
    ) -> Result<Vec<Job>> {
        self.do_consume(vec![queue], ttr, timeout, count).await
    }

    /// Consume from multiple queues. Note that the order of the queues in the params
    /// implies the priority. eg. consume_from_queues(120, 5, vec!("queue-a", "queue-b", "queue-c"))
    /// if all the queues have jobs to be fetched, the job in `queue-a` will be return.
    ///
    /// # Arguments
    ///
    /// * `queues` - A string vector that holds the queues for consuming
    ///
    /// * `ttr` - A u32 value that holds the time-to-run value in second.
    /// If the job is not finished before the `ttr` expires, the job will be
    /// released for consuming again if the `(tries-1) > 0`
    ///
    /// * `timeout` - A u32 value that holds the max waiting time for long polling
    /// If it's zero, this method will return immediately with or without a job;
    /// if it's positive, this method would polling for new job until timeout.
    ///
    /// * `count` - A u32 value that holds the count of wanted jobs
    pub async fn consume_from_queues(
        &self,
        queues: Vec<String>,
        ttr: u32,
        timeout: u32,
    ) -> Result<Vec<Job>> {
        self.do_consume(queues, ttr, timeout, 1).await
    }

    /// Mark a job as finished, so it won't be retried by others
    ///
    /// # Arguments
    ///
    /// * `queue` - A string that holds the queue for the job
    /// * `job_id` - A string that holds the job id
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
            });
        }

        let (request_id, response) = ret.unwrap();
        let status_code = response.status();

        match status_code {
            StatusCode::NO_CONTENT => Ok(()),
            _ => Err(APIError {
                err_type: ErrType::ResponseErr,
                reason: self.parse_response_error(response).await,
                job_id: job_id,
                request_id: request_id,
            }),
        }
    }

    /// Get the queue size.
    /// How many pending jobs are ready for consuming.
    ///
    /// # Arguments
    /// * `queue` - A string that holds the queue name
    pub async fn queue_size(&self, queue: String) -> Result<u32> {
        let relative_path = Path::new(&queue)
            .join("size")
            .to_str()
            .unwrap()
            .to_string();

        let ret = self
            .request::<(String, u32)>(Method::GET, relative_path.as_str(), None, None)
            .await;

        if ret.is_err() {
            return Err(APIError {
                err_type: ErrType::RequestErr,
                reason: ret.unwrap_err().to_string(),
                ..APIError::default()
            });
        }

        let (request_id, response) = ret.unwrap();
        let status_code = response.status();

        match status_code {
            StatusCode::OK => response
                .json::<QueueInfo>()
                .await
                .map(|info| info.size)
                .map_err(|e| APIError {
                    err_type: ErrType::ResponseErr,
                    reason: e.to_string(),
                    request_id: request_id,
                    ..APIError::default()
                }),
            _ => Err(APIError {
                err_type: ErrType::ResponseErr,
                reason: self.parse_response_error(response).await,
                job_id: "".to_string(),
                request_id: request_id,
            }),
        }
    }

    /// Peek job from queue without consuming
    ///
    /// # Arguments
    ///
    /// * `queue` - A string that holds the queue name
    pub async fn peek_queue(&self, queue: String) -> Result<Option<Job>> {
        self.do_peek_job(queue, None).await
    }

    /// Peek a specified job
    ///
    /// # Arguments
    ///
    /// * queue - A string that holds the queue name
    /// * job_id - A string that holds the job id
    pub async fn peek_job(&self, queue: String, job_id: String) -> Result<Option<Job>> {
        self.do_peek_job(queue, Some(job_id)).await
    }

    /// Peek the deadletter of the queue
    ///
    /// # Arguments
    ///
    /// * `queue` - A string that holds the queue name
    pub async fn peek_dead_letter(&self, queue: String) -> Result<(u32, String)> {
        let relative_path = Path::new(&queue)
            .join("deadletter")
            .to_str()
            .unwrap()
            .to_string();

        let ret = self
            .request::<(String, u32)>(Method::GET, relative_path.as_str(), None, None)
            .await;

        if ret.is_err() {
            return Err(APIError {
                err_type: ErrType::RequestErr,
                reason: ret.unwrap_err().to_string(),
                ..APIError::default()
            });
        }

        let (request_id, response) = ret.unwrap();
        let status_code = response.status();
        match status_code {
            StatusCode::OK => response
                .json::<DeadLetterInfo>()
                .await
                .map(|info| (info.deadletter_size, info.deadletter_head))
                .map_err(|e| APIError {
                    err_type: ErrType::ResponseErr,
                    reason: e.to_string(),
                    request_id: request_id,
                    ..APIError::default()
                }),
            _ => Err(APIError {
                err_type: ErrType::ResponseErr,
                reason: self.parse_response_error(response).await,
                job_id: "".to_string(),
                request_id: request_id,
            }),
        }
    }

    /// Respawn dead letter
    ///
    /// # Arguments
    /// * `queue` - A string that holds the queue name
    /// * `limit` - A i64 value that holds the limit
    /// * `ttl` - A i64 value that holds the time-to-live value in second
    pub async fn respawn_dead_letter(&self, queue: String, limit: i64, ttl: i64) -> Result<u32> {
        if limit <= 0 {
            return Err(APIError {
                err_type: ErrType::RequestErr,
                reason: "limit should be > 0".to_string(),
                ..APIError::default()
            });
        }

        if ttl <= 0 {
            return Err(APIError {
                err_type: ErrType::RequestErr,
                reason: "ttl should be > 0".to_string(),
                ..APIError::default()
            });
        }

        let relative_path = Path::new(&queue)
            .join("deadletter")
            .to_str()
            .unwrap()
            .to_string();

        let query = [("limit", limit), ("ttl", ttl)];
        let ret = self
            .request(Method::PUT, relative_path.as_str(), Some(&query), None)
            .await;

        if ret.is_err() {
            return Err(APIError {
                err_type: ErrType::RequestErr,
                reason: ret.unwrap_err().to_string(),
                ..APIError::default()
            });
        }

        let (request_id, response) = ret.unwrap();
        let status_code = response.status();
        match status_code {
            StatusCode::OK => response
                .json::<RespawnResult>()
                .await
                .map(|rr| rr.count)
                .map_err(|e| APIError {
                    err_type: ErrType::ResponseErr,
                    reason: e.to_string(),
                    request_id: request_id,
                    ..APIError::default()
                }),
            _ => Err(APIError {
                err_type: ErrType::ResponseErr,
                reason: self.parse_response_error(response).await,
                job_id: "".to_string(),
                request_id: request_id,
            }),
        }
    }
}
