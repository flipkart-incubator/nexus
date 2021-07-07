# Nexus
Nexus replicates arbitrary blobs of data across wide area networks (WANs) using
the [Raft consensus algorithm](https://raft.github.io/) by [etcd/raft](https://github.com/etcd-io/etcd/tree/master/raft)
onto pluggable storage backends.

It is intended to be used as a library for implementing synchronous replication
of data onto any given storage backend. Checkout the [examples](https://github.com/flipkart-incubator/nexus/raw/master/examples) directory for how this
can be achieved with few data stores.

## Features
- Synchronous replication of user-defined datasets
- Support for linearizable reads
- Support for addition & removal of replicas at runtime
- Periodic data consistency checks across replicas [TODO]

## Dependencies
- Go version 1.13+
- [Raft consensus algorithm](https://raft.github.io/) by [etcd/raft](https://github.com/etcd-io/etcd/tree/master/raft) version 3.3+

## Building Nexus

```bash
$ git clone https://github.com/flipkart-incubator/nexus
$ cd nexus
$ make build
```

If you want to build for other platforms, set `GOOS`, `GOARCH` environment variables. For example, build for macOS like following:

```bash
$ make GOOS=linux build
```

## Running the examples

Once Nexus is built, the `<PROJECT_ROOT>/bin` folder should contain the following binaries:
- `mysql_repl` - Service for sync replication onto MySQL datastore
- `redis_repl` - Service for sync replication onto Redis datastore
- `repl`       - Command line utility for playing with the example sync replication services

### Running the sync replication service for Redis

The example sync replication service for Redis can be used to execute arbitrary Lua statements/scripts
over multiple independent Redis instances. It exposes a GRPC endpoint using which Lua statements can be
issued for synchronous replication. The `repl` command line utility can be used to interact with this
service for easy verification. One can also build a standard GRPC client to interact with this endpoint.

Assuming 3 Redis instances running on ports `6379`, `6380` and `6381` on the local host, we now configure
the example sync replication service to make changes to these 3 keyspaces synchronously.

Launch the following 3 commands in separate terminal sessions:
```bash
$ <PROJECT_ROOT>/bin/redis_repl \
      -nexus-cluster-url "http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023" \
      -nexus-node-url "http://127.0.0.1:9021" \
      -grpcPort 9121 \
      -redisPort 6379
$ <PROJECT_ROOT>/bin/redis_repl \
      -nexus-cluster-url "http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023" \
      -nexus-node-url "http://127.0.0.1:9022" \
      -grpcPort 9122 \
      -redisPort 6380
$ <PROJECT_ROOT>/bin/redis_repl \
      -nexus-cluster-url "http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023" \
      -nexus-node-url "http://127.0.0.1:9023" \
      -grpcPort 9123 \
      -redisPort 6381
```

In a separate terminal session, launch the `repl` utility:
```bash
$ <PROJECT_ROOT>/bin/repl 127.0.0.1:9121 redis save db.index=2 "return redis.call('set', 'hello', 'world')"
Response from Redis (without quotes): 'OK'
$ <PROJECT_ROOT>/bin/repl 127.0.0.1:9121 redis save db.index=2 "return redis.call('incr', 'ctr')"
Response from Redis (without quotes): '1'
$ <PROJECT_ROOT>/bin/repl 127.0.0.1:9121 redis save db.index=2 "return redis.call('incr', 'ctr')"
Response from Redis (without quotes): '2'
$ <PROJECT_ROOT>/bin/repl 127.0.0.1:9121 redis load db.index=2 "return redis.call('keys', '*')"
Response from Redis (without quotes): '[ctr hello]'
$ <PROJECT_ROOT>/bin/repl 127.0.0.1:9121 redis load db.index=2 "return redis.call('get', 'ctr')"
Response from Redis (without quotes): '2'
```

Any valid command can be issued to Redis as a Lua statement. Please refer to [EVAL](https://redis.io/commands/eval) for more details.
Subsequently, each of the Redis instances can be inspected for these keys set via the `repl` utility.

### Running the sync replication service for MySQL

The example sync replication service for MySQL can be used to execute arbitrary SQL statements/scripts
over multiple MySQL replicas. Note that each of these replicas are independent MySQL instances and know
nothing about each other. The replication service exposes a GRPC endpoint through which SQL statements
can be submitted for synchronous replication. As in the previous example, the `repl` utility can be used
to interact with this service.

Assuming 3 MySQL instances running on ports `33061`, `33062` and `33063` on the local host, we now configure
the example sync replication service to make changes to these 3 database instances synchronously. For these
examples to work, please first create a database named `nexus` in each of the 3 MySQL instances.

Launch the following 3 commands in separate terminal sessions:
```bash
$ <PROJECT_ROOT>/bin/mysql_repl \
      -nexus-cluster-url="http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023" \
      -nexus-node-url="http://127.0.0.1:9021" \
      -grpcPort=9121 \
      -mysqlConnUrl "root:root@tcp(127.0.0.1:33061)/nexus?autocommit=false"
$ <PROJECT_ROOT>/bin/mysql_repl \
      -nexus-cluster-url="http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023" \
      -nexus-node-url="http://127.0.0.1:9022" \
      -grpcPort=9122 \
      -mysqlConnUrl "root:root@tcp(127.0.0.1:33062)/nexus?autocommit=false"
$ <PROJECT_ROOT>/bin/mysql_repl \
      -nexus-cluster-url="http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023" \
      -nexus-node-url="http://127.0.0.1:9023" \
      -grpcPort=9123 \
      -mysqlConnUrl "root:root@tcp(127.0.0.1:33063)/nexus?autocommit=false"
```

In a separate terminal session, launch the `repl` utility:
```bash
# Create a `sync_table` in all the nodes
$ <PROJECT_ROOT>/bin/repl 127.0.0.1:9121 mysql save "create table sync_table (id INT PRIMARY KEY AUTO_INCREMENT, data VARCHAR(50) NOT NULL, ts timestamp(3) default current_timestamp(3) on update current_timestamp(3));"

# Insert some data into this table
$ <PROJECT_ROOT>/bin/repl 127.0.0.1:9121 mysql save "insert into sync_table (name, data) values ('foo', 'bar');"
$ <PROJECT_ROOT>/bin/repl 127.0.0.1:9121 mysql save "insert into sync_table (name, data) values ('hello', 'world');"

# Load some data from this table
$ <PROJECT_ROOT>/bin/repl 127.0.0.1:9121 mysql load "select * from nexus.sync_table;"
```

Each of the 3 MySQL instances can now be inspected to ensure the table `sync_table` is created and it
contains 2 rows in it. Likewise any arbitrary SQL statements can be issued for synchronous replication
to all the 3 MySQL instances.

### Changing Nexus cluster membership

At runtime, nodes belonging to an existing Nexus cluster can be removed or new nodes added, using the procedure described below.

```bash
# Launch a node listening at a specific address
$ <PROJECT_ROOT>/bin/redis_repl \
      -nexus-cluster-url "http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023" \
      -nexus-node-url "http://127.0.0.1:9024" \
      -grpcPort 9124 \
      -redisPort 6382 
# Add this node to the existing cluster
$ <PROJECT_ROOT>/bin/repl 127.0.0.1:9121 addNode "http://127.0.0.1:9024"
Current cluster members:
4c8e64e91e16db7c => http://127.0.0.1:9024
663ca4baed7470b5 => http://127.0.0.1:9022
a10202ab0eac4801 => http://127.0.0.1:9021 (leader)
f3fd5b0446cc7033 => http://127.0.0.1:9023
```

The first command makes the cluster aware of the new node that is about to join the cluster. While the second command launches this new node.

Alternatively, nodes can be removed from an existing Nexus cluster. It does not matter even if the node about to be removed happens to be the leader of the cluster during that time. In such a case, the node removal is automatically followed by a leader election.

```bash
# Remove node with URL "http://127.0.0.1:9022"
$ <PROJECT_ROOT>/bin/repl 127.0.0.1:9121 removeNode "http://127.0.0.1:9022"
Current cluster members:
4c8e64e91e16db7c => http://127.0.0.1:9024
a10202ab0eac4801 => http://127.0.0.1:9021 (leader)
f3fd5b0446cc7033 => http://127.0.0.1:9023
```

### Reads based on leader leases

By default, reads performed over Nexus perform a round trip with all the replicas and results are returned based on the quorum. This is done to provide linearizable guarantees. However, if the performance cost of this round trip during read time is undesirable, the flag `-nexusLeaseBasedReads` can be used to avoid it and instead return the local copy so long as the lease is active on the serving node. Periodically messages are exchanged with other replicas so as to ensure the current lease is still active.

Note that in environments where there can be unbounded clock drifts, this lease based approach can return stale results (non-linearizable) when the lease holder assumes its lease validity longer than it should, based on its local clock.

## Testing

If you want to execute tests inside Nexus, run this command:

```bash
$ make test
```

## Packaging

###  Linux

```bash
$ make GOOS=linux dist
```

### macOS

```bash
$ make GOOS=darwin dist
```

