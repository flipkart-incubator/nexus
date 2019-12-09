# nexus

Nexus replicates arbitrary blobs of data across wide area networks (WANs) using
the [Raft consensus algorithm](https://raft.github.io/) by [etcd/raft](https://github.com/etcd-io/etcd/tree/master/raft)
onto pluggable storage backends.

It is intended to be used as a library for implementing synchronous replication
of data onto any given storage backend. Checkout the [examples](https://github.com/flipkart-incubator/nexus/raw/master/examples) directory for how this
can be achieved with few data stores.

[Coming Soon]
