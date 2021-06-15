# Redis Nexus Replication


## Starting a simple local cluster

- Ensure that you have `goreman` installed. If no install it by running `go get github.com/mattn/goreman`
- Start the nexus cluster `goreman start`
- Read/Write via the `repl` cli. 

```bash
bin/repl 127.0.0.1:9121 redis save "return redis.call('set', 'hello', 'world')" #write
bin/repl 127.0.0.1:9122 redis load "return redis.call('get', 'hello')" #read
```