# nexus

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

[Coming Soon]
