#[cfg(test)]
use crate::*;

#[tokio::test]
async fn publish_to_unexpected_server_should_return_error() {
    let namespace = "niuniu";
    let token = "this_is_my_token";
    let host = "baidu.com";
    let port = 80;
    let retry = 3;
    let back_off = 10;

    let client = api::Client::new(namespace, token, host, port, retry, back_off);
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
