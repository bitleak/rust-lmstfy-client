#[cfg(test)]
use crate::*;
use base64::decode;
use serde::{Deserialize, Serialize};
use serde_json::{Result};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::{thread};

#[derive(Deserialize, Serialize)]
struct Config {
    /// The namespace for the client
    pub namespace: String,
    /// The access token of api
    pub token: String,
    /// The target host for the server
    pub host: String,
    /// The target port for the server
    pub port: u32,
    /// The suffix of queue
    pub queue_suffix: String,
}

/// Get config values from environment vars
fn get_config_from_env() -> Result<Config> {
    assert_eq!(
        true,
        envmnt::exists("LMSTFY_CLIENT_TEST_CONFIG"),
        "environment var LMSTFY_CLIENT_TEST_CONFIG not exist"
    );
    let data = envmnt::get_or_panic("LMSTFY_CLIENT_TEST_CONFIG");
    println!("Env Value: {}", &data);
    let config: Config = serde_json::from_str(&data)?;
    assert_ne!(config.namespace, "", "namespace should not be empty");
    assert_ne!(config.token, "", "token should not be empty");
    assert_ne!(config.host, "", "host should not be empty");
    assert_ne!(config.port, 0, "port should not be zero");
    assert_ne!(config.queue_suffix, "", "suffix should not be empty");

    Ok(config)
}

macro_rules! get_config {
    () => {{
        let ret = get_config_from_env();
        assert_eq!(true, ret.is_ok());
        ret.unwrap()
    }};
}

fn new_client(retry: u32, back_off: u32) -> api::Client {
    let config = get_config!();
    api::Client::new(
        &config.namespace,
        &config.token,
        &config.host,
        config.port,
        retry,
        back_off,
    )
}

fn new_queue(name: &str) -> String {
    let config = get_config!();
    [name, &config.queue_suffix].join("-")
}

async fn drain_jobs(client: api::Client, queue: String, count: usize) {
    for _ in 0..count {
        let _ret = client.consume(queue.clone(), 10, 0).await.map_err(|e| {
            println!("err: {:#?}", e);
            assert!(false, "should succeed");
        });
    }
}

#[test]
fn test_env_should_be_ok() {
    let _config = get_config!();
}

#[tokio::test]
async fn publish_message_should_be_ok() {
    let client = new_client(3, 10);
    let queue = new_queue("rustqueue-publish");
    let _ret = client
        .publish(queue.clone(), "".to_string(), vec![1, 2, 3], 0, 1, 0)
        .await
        .map(|ret| {
            println!("results: {:#?}", ret);
        })
        .map_err(|e| {
            println!("err: {:#?}", e);
            assert!(false, "should succeed");
        });
}

#[tokio::test]
async fn republish_message_should_be_ok() {
    let client = new_client(3, 10);
    let queue = new_queue("rustqueue-republish");
    let message = b"hello";
    let ret = client
        .publish(queue.clone(), "".to_string(), message.to_vec(), 0, 1, 0)
        .await
        .map_err(|e| {
            println!("err: {:#?}", e);
            assert!(false, "should succeed");
        });
    let (job_id, _) = ret.unwrap();
    let ret = client.consume(queue.clone(), 5, 1).await;
    let ret = ret.unwrap();
    assert_eq!(ret.len(), 1, "should consume a message");
    let job = &ret[0];
    let decoded_message = &decode(job.data.clone()).unwrap()[..];
    assert_eq!(message, decoded_message, "message content should match");
    assert_eq!(job_id, job.job_id);
    // Publish it again
    let ret = client
        .publish(
            queue.clone(),
            job_id, // republish
            message.to_vec(),
            10,
            1,
            0,
        )
        .await
        .map_err(|e| {
            println!("err: {:#?}", e);
            assert!(false, "should succeed");
        });
    let (job_id, _) = ret.unwrap();
    // Consume again
    let ret = client.consume(String::from(queue), 5, 1).await;
    let ret = ret.unwrap();
    assert_eq!(ret.len(), 1, "should consume a message");
    let job = &ret[0];
    let decoded_message = &decode(job.data.clone()).unwrap()[..];
    assert_eq!(message, decoded_message, "message content should match");
    assert_eq!(job_id, job.job_id);
    assert_eq!(10, job.ttl);
}

#[tokio::test]
async fn consume_message_should_be_ok() {
    let client = new_client(3, 10);
    let queue = new_queue("rustqueue-consume");
    let message = b"hello, world";
    let _ret = client
        .publish(queue.clone(), "".to_string(), message.to_vec(), 100, 3, 0)
        .await
        .map_err(|e| {
            println!("err: {:#?}", e);
            assert!(false, "should succeed");
        });

    let ret = client.consume(queue.clone(), 3, 10).await;
    let ret = ret.unwrap();
    assert_eq!(ret.len(), 1, "should consume a message");
    let job = &ret[0];
    let decoded_message = &decode(job.data.clone()).unwrap()[..];
    assert_eq!(message, decoded_message, "message content should match");

    println!("results: {:#?}", ret);
    println!(
        "decoded_message: {:#?}",
        String::from_utf8(decoded_message.to_vec()).unwrap()
    );
}

#[tokio::test]
async fn consume_message_should_return_empty_when_not_found() {
    let client = new_client(3, 10);
    let queue = new_queue("rustqueue-consume-empty");

    let ret = client.consume(queue, 3, 10).await;
    let ret = ret.unwrap();
    assert_eq!(ret.len(), 0, "should be empty");
}

#[tokio::test]
async fn consume_multiple_messages_should_be_ok() {
    let client = new_client(3, 10);
    let queue = new_queue("rustqueue-consume-multiple");
    let mut job_map = HashMap::new();
    let messages = [b"foo", b"bar", b"hah", b"heh"];
    // Firstly, publish 4 messages
    for &msg in &messages {
        let _ret = client
            .publish(queue.clone(), "".to_string(), msg.to_vec(), 0, 1, 0)
            .await
            .map(|(job_id, _request_id)| {
                job_map.insert(job_id, msg);
            })
            .map_err(|e| {
                println!("err: {:#?}", e);
                assert!(false, "should succeed");
            });
    }
    // Then, consume 3 messages
    let _ret = client
        .batch_consume(queue.clone(), 3, 3, 3)
        .await
        .map(|jobs| {
            assert_eq!(3, jobs.len(), "jos length not match");
            for job in jobs {
                let decoded_message = &decode(job.data.clone()).unwrap()[..];
                assert_eq!(
                    job_map.get(&job.job_id).unwrap(),
                    &decoded_message,
                    "message content should match"
                );
            }
        })
        .map_err(|e| {
            println!("err: {:#?}", e);
            assert!(false, "should succeed");
        });
    // Last, consume the last one
    let _ret = client
        .batch_consume(queue.clone(), 3, 3, 3)
        .await
        .map(|jobs| {
            assert_eq!(1, jobs.len(), "jos length not match");
            for job in jobs {
                let decoded_message = &decode(job.data.clone()).unwrap()[..];
                assert_eq!(
                    job_map.get(&job.job_id).unwrap(),
                    &decoded_message,
                    "message content should match"
                );
            }
        })
        .map_err(|e| {
            println!("err: {:#?}", e);
            assert!(false, "should succeed");
        });

    // Test timeout
    let now = Instant::now();
    let _ret = client
        .batch_consume(queue.clone(), 3, 3, 3)
        .await
        .map(|jobs| {
            assert_eq!(0, jobs.len(), "jos length not match");
        })
        .map_err(|e| {
            println!("err: {:#?}", e);
            assert!(false, "should succeed");
        });
    assert_eq!(true, now.elapsed().as_secs() < 10);
}

#[tokio::test]
async fn consume_from_queues_should_be_ok() {
    let client = new_client(3, 10);
    let pairs = [
        (new_queue("rustqueue1"), b"hello1"),
        (new_queue("rustqueue2"), b"hello2"),
    ];
    let mut last_job_id = String::from("");
    // Firstly, publish 4 messages
    for (queue, msg) in pairs.iter() {
        let _ret = client
            .publish(String::from(queue), "".to_string(), msg.to_vec(), 0, 1, 0)
            .await
            .map(|(job_id, _request_id)| {
                last_job_id = job_id.clone();
            })
            .map_err(|e| {
                println!("err: {:#?}", e);
                assert!(false, "should succeed");
            });
    }

    println!("last job id: {:#?}", last_job_id);
    let mut queues = pairs
        .iter()
        .map(|(queue, _msg)| queue.clone())
        .collect::<Vec<String>>();
    // Reverse queue vector
    queues.reverse();
    let _ret = client
        .consume_from_queues(queues, 10, 1)
        .await
        .map(|jobs| {
            let job = &jobs[0];
            let decoded_message = &decode(job.data.clone()).unwrap()[..];
            assert_eq!(b"hello2", decoded_message, "message content should match");
        })
        .map_err(|e| {
            println!("err: {:#?}", e);
            assert!(false, "should succeed");
        });
}

#[tokio::test]
async fn get_queue_size_should_be_ok() {
    let client = new_client(3, 10);
    let queue = new_queue("rustqueue-size");
    let messages = [b"hello1", b"hello2"];
    // Publish some messages
    for &msg in &messages {
        let _ret = client
            .publish(queue.clone(), "".to_string(), msg.to_vec(), 0, 1, 0)
            .await
            .map_err(|_| {
                assert!(false, "should succeed");
            });
    }
    // Check queue size
    let _ret = client
        .queue_size(queue.clone())
        .await
        .map(|count| {
            assert_eq!(messages.len(), count as usize, "queue size should match");
        })
        .map_err(|_| {
            assert!(false, "should succeed");
        });
    // Drain jobs
    drain_jobs(client, queue.clone(), messages.len()).await;
}

#[tokio::test]
async fn peek_queue_should_ok() {
    let client = new_client(3, 10);
    let queue = new_queue("rustqueue-peek");
    let message = b"hello";
    // Publish some messages
    let ret = client
        .publish(queue.clone(), "".to_string(), message.to_vec(), 0, 1, 0)
        .await
        .map_err(|_| {
            assert!(false, "should succeed");
        });
    let (job_id, _) = ret.unwrap();
    // Peek a job from queue
    let ret = client.peek_queue(queue.clone()).await.map_err(|_| {
        assert!(false, "should succeed");
    });
    let job = ret.unwrap().unwrap();
    assert_eq!(job_id, job.job_id);
    let decoded_message = &decode(job.data.clone()).unwrap()[..];
    assert_eq!(message, decoded_message, "message content should match");
    // Check queue size
    let _ret = client
        .queue_size(queue.clone())
        .await
        .map(|count| {
            assert_eq!(1, count as usize, "queue size should match");
        })
        .map_err(|_| {
            assert!(false, "should succeed");
        });
    // Drain jobs
    drain_jobs(client, queue.clone(), 1).await;
}

#[tokio::test]
async fn peek_job_should_ok() {
    let client = new_client(3, 10);
    let queue = new_queue("rustqueue-peek-job");
    let message = b"hello";
    // Publish some messages
    let ret = client
        .publish(queue.clone(), "".to_string(), message.to_vec(), 0, 1, 0)
        .await
        .map_err(|_| {
            assert!(false, "should succeed");
        });
    let (job_id, _) = ret.unwrap();
    // Peek a job from queue
    let ret = client
        .peek_job(queue.clone(), job_id.clone())
        .await
        .map_err(|_| {
            assert!(false, "should succeed");
        });
    let job = ret.unwrap().unwrap();
    assert_eq!(job_id, job.job_id);
    let decoded_message = &decode(job.data.clone()).unwrap()[..];
    assert_eq!(message, decoded_message, "message content should match");
    // Check queue size
    let _ret = client
        .queue_size(queue.clone())
        .await
        .map(|count| {
            assert_eq!(1, count as usize, "queue size should match");
        })
        .map_err(|_| {
            assert!(false, "should succeed");
        });

    // Drain jobs
    drain_jobs(client, queue.clone(), 1).await;
}

#[tokio::test]
async fn peek_dead_letter_should_work() {
    let client = new_client(3, 10);
    let queue = new_queue("rustqueue-peek-dead-letter");
    let message = b"hello";
    // Publish some messages
    let ret = client
        .publish(queue.clone(), "".to_string(), message.to_vec(), 0, 1, 0)
        .await
        .map_err(|_| {
            assert!(false, "should succeed");
        });
    let (job_id, _) = ret.unwrap();
    // Consume it
    let ret = client.consume(queue.clone(), 1, 0).await;
    let ret = ret.unwrap();
    assert_eq!(ret.len(), 1, "should consume a message");
    // Sleep until TTR expires
    thread::sleep(Duration::from_secs(2));
    // Peek a job from queue
    let ret = client.peek_dead_letter(queue.clone()).await.map_err(|e| {
        println!("{}", e);
        assert!(false, "should succeed");
    });
    let (size, dead_job_id) = ret.unwrap();
    assert_eq!(size, 1);
    assert_eq!(job_id, dead_job_id);
    // Drain jobs
    drain_jobs(client, queue.clone(), 1).await;
}

#[tokio::test]
async fn respawn_dead_letter_should_work() {
    let client = new_client(3, 10);
    let queue = new_queue("rustqueue-respawn-dead-letter");
    let message = b"hello";
    // Publish some messages
    let ret = client
        .publish(queue.clone(), "".to_string(), message.to_vec(), 0, 1, 0)
        .await
        .map_err(|_| {
            assert!(false, "should succeed");
        });
    let (job_id, _) = ret.unwrap();
    // Consume it
    let ret = client.consume(queue.clone(), 1, 0).await;
    let ret = ret.unwrap();
    assert_eq!(ret.len(), 1, "should consume a message");
    // Sleep until TTR expires
    thread::sleep(Duration::from_secs(2));
    // Respawn the deadletter
    let ret = client
        .respawn_dead_letter(queue.clone(), 2, 120)
        .await
        .map_err(|e| {
            println!("{}", e);
            assert!(false, "should succeed");
        });
    let count = ret.unwrap();
    assert_eq!(count, 1);
    // Reconsume it
    let ret = client.consume(queue.clone(), 1, 0).await;
    let ret = ret.unwrap();
    assert_eq!(ret.len(), 1, "should consume a message");
    let job = &ret[0];
    let decoded_message = &decode(job.data.clone()).unwrap()[..];
    assert_eq!(message, decoded_message, "message content should match");
    assert_eq!(job_id, job.job_id);
    drain_jobs(client, queue.clone(), 1).await;
}

#[tokio::test]
async fn ack_should_be_ok() {
    let client = new_client(3, 10);
    let queue = new_queue("rustqueue-ack");
    let ret = client
        .publish(queue.to_string(), "".to_string(), vec![1, 2, 3], 0, 1, 0)
        .await;
    let (job_id, _request_id) = ret.unwrap();
    let ret = client.consume(queue.clone(), 10, 0).await;
    let jobs = ret.unwrap();
    let job = &jobs[0];
    assert_eq!(job_id, job.job_id);
    let _ret = client
        .ack(queue.clone(), job.job_id.clone())
        .await
        .map(|v| {
            assert_eq!((), v);
        })
        .map_err(|_| {
            assert!(false, "should succeed");
        });
}