# lmstfy-client

lmstfy is a task queue service, created by meitu team.

# How to test
* export config
```
$ export LMSTFY_CLIENT_TEST_CONFIG='{"namespace": "myns", "token": "imtoken", "host":"localhost", "port": 6666}'
```
* cargo test