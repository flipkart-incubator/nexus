# Nexus
Nexus replicates arbitrary blobs of data across wide area networks (WANs) using
the [Raft consensus algorithm](https://raft.github.io/) by [etcd/raft](https://github.com/etcd-io/etcd/tree/master/raft)
onto pluggable storage backends.

It is intended to be used as a library for implementing synchronous replication
of data onto any given storage backend. Checkout the [examples](https://github.com/flipkart-incubator/nexus/raw/master/examples) directory for how this
can be achieved with few data stores.

## Features
- Synchronous replication of user-defined datasets
- Support for addition & removal of replicas at runtime
- Periodic data consistency checks across replicas [TODO]

## Dependencies
- Go version 1.13+
- [Raft consensus algorithm](https://raft.github.io/) by [etcd/raft](https://github.com/etcd-io/etcd/tree/master/raft) version 3.3+

## Building Nexus

```bash
$ mkdir -p ${GOPATH}/src/github.com/flipkart-incubator
$ cd ${GOPATH}/src/github.com/flipkart-incubator
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
      -nexusPort 9121 \
      -nexusClusterUrl "http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023" \
      -nexusNodeId 1 \
      -redisPort 6379
$ <PROJECT_ROOT>/bin/redis_repl \
      -nexusPort 9122 \
      -nexusClusterUrl "http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023" \
      -nexusNodeId 2 \
      -redisPort 6380
$ <PROJECT_ROOT>/bin/redis_repl \
      -nexusPort 9123 \
      -nexusClusterUrl "http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023" \
      -nexusNodeId 3 \
      -redisPort 6381
```

In a separate terminal session, launch the `repl` utility:
```bash
$ <PROJECT_ROOT>/bin/repl redis 127.0.0.1:9121
redis> redis.call('set', 'hello', 'world')
OK
redis> redis.call('set', 'foo', 'bar')
OK
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
      -nexusPort=9121 \
      -nexusClusterUrl="http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023" \
      -nexusNodeId=1 \
      -mysqlConnUrl "root:root@tcp(127.0.0.1:33061)/nexus?autocommit=false"
$ <PROJECT_ROOT>/bin/mysql_repl \
      -nexusPort=9122 \
      -nexusClusterUrl="http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023" \
      -nexusNodeId=2 \
      -mysqlConnUrl "root:root@tcp(127.0.0.1:33062)/nexus?autocommit=false"
$ <PROJECT_ROOT>/bin/mysql_repl \
      -nexusPort=9123 \
      -nexusClusterUrl="http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023" \
      -nexusNodeId=3 \
      -mysqlConnUrl "root:root@tcp(127.0.0.1:33063)/nexus?autocommit=false"
```

In a separate terminal session, launch the `repl` utility:
```bash
$ <PROJECT_ROOT>/bin/repl mysql 127.0.0.1:9121
mysql> create table sync_table (id INT PRIMARY KEY AUTO_INCREMENT, data VARCHAR(50) NOT NULL, ts timestamp(3) default current_timestamp(3) on update current_timestamp(3));
OK
mysql> insert into sync_table (data) values ('hello world');
OK
mysql> insert into sync_table (data) values ('foo bar');
OK
```

Each of the 3 MySQL instances can now be inspected to ensure the table `sync_table` is created and it
contains 2 rows in it. Likewise any arbitrary SQL statements can be issued for synchronous replication
to all the 3 MySQL instances.

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

