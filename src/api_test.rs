#[cfg(test)]
use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::{Result, Value};

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

    let retry = 3;
    let back_off = 10;

    api::Client::new(
        &config.namespace,
        &config.token,
        &config.host,
        config.port,
        retry,
        back_off,
    )
}

#[test]
fn test_env_should_be_ok() {
    let _config = get_config!();
}

#[tokio::test]
async fn publish_message_should_be_ok() {
    let client = new_client(3, 10);
    let ret = client
        .publish(
            "rustqueue".to_string(),
            "".to_string(),
            vec![1, 2, 3],
            100,
            3,
            0,
        )
        .await;
    assert_eq!(ret.is_ok(), true, "should succeed");
    println!("results: {:#?}", ret.unwrap());
}

#[tokio::test]
async fn consume_message_should_be_ok() {
    let client = new_client(3, 10);
    let queue = "rustqueue";
    let message = "hello, world";
    let ret = client
        .publish(
            String::from(queue),
            "".to_string(),
            message.as_bytes().to_vec(),
            100,
            3,
            0,
        )
        .await;
    assert_eq!(ret.is_ok(), true, "should succeed"); 

    let ret = client
        .consume(String::from(queue), 3, 10)
        .await;
    let ret = ret.unwrap();
    assert_eq!(ret.len(), 1, "should consume a message");

    println!("results: {:#?}", ret);
}

#[tokio::test]
async fn publish_to_unexpected_server_should_return_error() {
    let config = get_config!();

    let retry = 3;
    let back_off = 10;

    let client = api::Client::new(
        &config.namespace,
        &config.token,
        &config.host,
        config.port,
        retry,
        back_off,
    );
    let ret = client
        .publish(
            "habit".to_string(),
            "".to_string(),
            vec![1, 2, 3],
            100,
            3,
            0,
        )
        .await;

    println!("ret = {:#?}", ret);
    assert_eq!(ret.unwrap_err().err_type, errors::ErrType::RequestErr);
}

#[tokio::test]
async fn ack_to_invalid_job_id_should_return_error() {
    let config = get_config!();
    let retry = 3;
    let back_off = 10;

    let client = api::Client::new(
        &config.namespace,
        &config.token,
        &config.host,
        config.port,
        retry,
        back_off,
    );
    let ret = client
        .ack("habit".to_string(), "invalid_job_id".to_string())
        .await;

    println!("ret = {:#?}", ret);
    assert_eq!(ret.unwrap_err().err_type, errors::ErrType::RequestErr);
}

#[tokio::test]
async fn consume_should_work() {}

#[tokio::test]
async fn batch_consume_should_work() {}

#[tokio::test]
async fn consume_from_queues_should_work() {}

#[tokio::test]
async fn queue_size_should_work() {}

#[tokio::test]
async fn peek_queue_should_work() {}

#[tokio::test]
async fn peek_job_should_work() {}

#[tokio::test]
async fn peek_dead_letter_should_work() {}

#[tokio::test]
async fn respawn_dead_letter_should_work() {}
