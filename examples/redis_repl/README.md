# Redis Nexus Replication


## Starting a simple local cluster

- Ensure that you have `goreman` installed. If not, install it by running `go get github.com/mattn/goreman`
- Start the nexus cluster `goreman start`
- Read/Write via the `repl` cli. 

```bash
bin/repl 127.0.0.1:9121 redis save db.index=2 "return redis.call('set', 'hello', 'world')" #write
bin/repl 127.0.0.1:9122 redis load db.index=2 "return redis.call('get', 'hello')" #read
```