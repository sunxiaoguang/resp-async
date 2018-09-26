Asynchronous Redis protocol (RESP) parser and framework for implementing server

All examples can be executed with:

run server
```
cargo run --example $name
```

to connect to server  use
```
redis-cli -h 127.0.0.1 -p 8080
```


A high level description of each example is:

* [`history`](history.rs) - a tiny server that always return history command the client requested.
