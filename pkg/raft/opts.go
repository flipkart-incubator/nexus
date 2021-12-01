package raft

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"go.etcd.io/etcd/raft/v3"
)

const (
	defaultSnapshotCount int64 = 10000
	// defaultSnapshotCatchUpEntries is the number of entries for a slow follower
	// to catch-up after compacting the raft storage entries.
	// We expect the follower has a millisecond level latency with the leader.
	// The max throughput is around 10K. Keep a 5K entries is enough for helping
	// follower to catch up.
	defaultSnapshotCatchUpEntries int64 = 5000

	defaultRaftReplTimeout = 5
	defaultMaxWAL          = 5
	defaultMaxSNAP         = 5
)

type Option func(*options) error

type Options interface {
	NodeId() uint64
	NodeUrl() *url.URL
	Join() bool
	LogDir() string
	SnapDir() string
	ClusterUrls() map[uint64]string
	ClusterId() uint64
	ReplTimeout() time.Duration
	ReadOption() raft.ReadOnlyOption
	StatsDAddr() string
	MaxSnapFiles() uint
	MaxWALFiles() uint
	SnapshotCount() uint64
	SnapshotCatchUpEntries() uint64
}

type options struct {
	nodeUrl                *url.URL
	nodeUrlStr             string
	logDir                 string
	snapDir                string
	clusterUrl             string
	clusterName            string
	clusterUrls            []*url.URL
	replTimeout            time.Duration
	leaseBasedReads        bool
	statsdAddr             string
	maxSnapFiles           int
	maxWALFiles            int
	snapshotCount          int64
	snapshotCatchUpEntries int64
}

var (
	opts              options
	replTimeoutInSecs int64
)

func init() {
	flag.StringVar(&opts.nodeUrlStr, "nexus-node-url", "", "Url for the Nexus service to be started on this node (format: http://<local_node>:<port_num>)")
	flag.StringVar(&opts.logDir, "nexus-log-dir", "/tmp/logs", "Dir for storing RAFT logs")
	flag.StringVar(&opts.snapDir, "nexus-snap-dir", "/tmp/snap", "Dir for storing RAFT snapshots")
	flag.StringVar(&opts.clusterUrl, "nexus-cluster-url", "", "Comma separated list of Nexus URLs of other nodes in the cluster")
	flag.StringVar(&opts.clusterName, "nexus-cluster-name", "", "Unique name of this Nexus cluster")
	flag.Int64Var(&replTimeoutInSecs, "nexus-repl-timeout", defaultRaftReplTimeout, "Replication timeout in seconds")
	flag.BoolVar(&opts.leaseBasedReads, "nexus-lease-based-reads", true, "Perform reads using RAFT leader leases")
	flag.StringVar(&opts.statsdAddr, "nexus-statsd-addr", "", "StatsD server address (host:port) for relaying various metrics")

	flag.IntVar(&opts.maxSnapFiles, "nexus-max-snapshots", defaultMaxSNAP, "Maximum number of snapshot files to retain (0 is unlimited)")
	flag.IntVar(&opts.maxWALFiles, "nexus-max-wals", defaultMaxWAL, "Maximum number of wal files to retain (0 is unlimited)")
	flag.Int64Var(&opts.snapshotCount, "nexus-snapshot-count", defaultSnapshotCount, "Number of committed transactions to trigger a snapshot to disk. (default 10K)")
	flag.Int64Var(&opts.snapshotCatchUpEntries, "nexus-snapshot-catchup-entries", defaultSnapshotCatchUpEntries, "Number of entries for a slow follower to catch-up after compacting the raft storage entries (Default 5K)")
}

func OptionsFromFlags() []Option {
	return []Option{
		LogDir(opts.logDir),
		SnapDir(opts.snapDir),
		ClusterUrl(opts.clusterUrl),
		NodeUrl(opts.nodeUrlStr),
		ReplicationTimeout(time.Duration(replTimeoutInSecs) * time.Second),
		LeaseBasedReads(opts.leaseBasedReads),
		StatsDAddr(opts.statsdAddr),
		MaxSnapFiles(opts.maxSnapFiles),
		MaxWALFiles(opts.maxWALFiles),
		SnapshotCount(opts.snapshotCount),
		SnapshotCatchUpEntries(opts.snapshotCatchUpEntries),
		ClusterName(opts.clusterName),
	}
}

func NewOptions(opts ...Option) (Options, error) {
	options := &options{}
	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}
	return options, nil
}

func (this *options) NodeId() uint64 {
	return this.hash(this.nodeUrl.Host)
}

func (this *options) hash(url string) uint64 {
	hash := sha1.Sum([]byte(url))
	return binary.BigEndian.Uint64(hash[:8])
}

func (this *options) NodeUrl() *url.URL {
	return this.nodeUrl
}

func (this *options) Join() bool {
	_, present := this.ClusterUrls()[this.NodeId()]
	return !present
}

func (this *options) LogDir() string {
	return fmt.Sprintf("%s/node_%x", this.logDir, this.NodeId())
}

func (this *options) SnapDir() string {
	return fmt.Sprintf("%s/node_%x", this.snapDir, this.NodeId())
}

func (this *options) ClusterUrls() map[uint64]string {
	res := make(map[uint64]string, len(this.clusterUrls))
	for _, nodeUrl := range this.clusterUrls {
		id := this.hash(nodeUrl.Host)
		res[id] = nodeUrl.String()
	}
	return res
}

func (this *options) StatsDAddr() string {
	return this.statsdAddr
}

func (this *options) ReplTimeout() time.Duration {
	return this.replTimeout
}

func (this *options) ReadOption() raft.ReadOnlyOption {
	if this.leaseBasedReads {
		return raft.ReadOnlyLeaseBased
	}
	return raft.ReadOnlySafe
}

func getLocalIPAddress() (map[string]net.IP, error) {
	var ips = make(map[string]net.IP)
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			switch v := addr.(type) {
			case *net.IPNet:
				ips[v.IP.String()] = v.IP
			case *net.IPAddr:
				ips[v.IP.String()] = v.IP
			}
		}
	}
	return ips, nil
}

func validateAndParseAddress(addr string) (*url.URL, error) {
	if nodeUrl, err := url.Parse(addr); err != nil {
		return nil, fmt.Errorf("given listen address, %s is not a valid URL, error: %v", addr, err)
	} else {
		if nodeUrl.Scheme != "http" {
			return nil, fmt.Errorf("given listen address, %s must have HTTP scheme", addr)
		}
		if !strings.ContainsRune(nodeUrl.Host, ':') {
			return nil, fmt.Errorf("given listen address, %s must include port number", addr)
		}
		return nodeUrl, nil
	}
}

func NodeUrl(addr string) Option {
	return func(opts *options) error {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			//try to auto determine nodeUrl from clusterUrl
			localIps, err := getLocalIPAddress()
			if err == nil {
				for _, clusterUrl := range opts.clusterUrls {
					//check if localIps contains this
					host, _, _ := net.SplitHostPort(clusterUrl.Host)
					if _, ok := localIps[host]; ok {
						opts.nodeUrl = clusterUrl
						return nil
					}
				}
			}
			return errors.New("nexus listen address not provided & auto detection failed")
		}
		if nodeUrl, err := validateAndParseAddress(addr); err != nil {
			return err
		} else {
			opts.nodeUrl = nodeUrl
		}
		return nil
	}
}

func LogDir(dir string) Option {
	return func(opts *options) error {
		dir = strings.TrimSpace(dir)
		if dir == "" {
			return errors.New("Raft log dir must not be empty")
		}
		opts.logDir = dir
		return nil
	}
}

func SnapDir(dir string) Option {
	return func(opts *options) error {
		dir = strings.TrimSpace(dir)
		if dir == "" {
			return errors.New("Raft snapshot dir must not be empty")
		}
		opts.snapDir = dir
		return nil
	}
}

func ClusterUrl(url string) Option {
	return func(opts *options) error {
		url = strings.TrimSpace(url)
		if url == "" {
			return errors.New("Raft cluster url must not be empty")
		}
		opts.clusterUrl = url
		nodes := strings.Split(opts.clusterUrl, ",")
		for _, node := range nodes {
			if nodeUrl, err := validateAndParseAddress(node); err != nil {
				return err
			} else {
				opts.clusterUrls = append(opts.clusterUrls, nodeUrl)
			}
		}
		return nil
	}
}

func ReplicationTimeout(timeout time.Duration) Option {
	return func(opts *options) error {
		if timeout <= 0 {
			return errors.New("Replication timeout must strictly be greater than 0")
		}
		opts.replTimeout = timeout
		return nil
	}
}

func LeaseBasedReads(leaseBasedReads bool) Option {
	return func(opts *options) error {
		opts.leaseBasedReads = leaseBasedReads
		return nil
	}
}

func StatsDAddr(statsdAddr string) Option {
	return func(opts *options) error {
		if statsdAddr = strings.TrimSpace(statsdAddr); statsdAddr != "" {
			opts.statsdAddr = statsdAddr
		}
		return nil
	}
}

func (this *options) MaxSnapFiles() uint {
	if this.maxSnapFiles == 0 {
		return defaultMaxSNAP
	}
	return uint(this.maxSnapFiles)
}

func (this *options) MaxWALFiles() uint {
	if this.maxWALFiles == 0 {
		return defaultMaxWAL
	}
	return uint(this.maxWALFiles)
}

func (this *options) SnapshotCount() uint64 {
	if this.snapshotCount == 0 {
		return uint64(defaultSnapshotCount)
	}
	return uint64(this.snapshotCount)
}

func (this *options) SnapshotCatchUpEntries() uint64 {
	if this.snapshotCatchUpEntries == 0 {
		return uint64(defaultSnapshotCatchUpEntries)
	}
	return uint64(this.snapshotCatchUpEntries)
}

func MaxSnapFiles(count int) Option {
	return func(opts *options) error {
		if count < 0 {
			return errors.New("maxSnapFiles cannot be negative")
		}
		opts.maxSnapFiles = count
		return nil
	}
}

func MaxWALFiles(count int) Option {
	return func(opts *options) error {
		if count < 0 {
			return errors.New("maxWALFiles cannot be negative")
		}
		opts.maxWALFiles = count
		return nil
	}
}

func SnapshotCount(count int64) Option {
	return func(opts *options) error {
		if count < 1 {
			return errors.New("snapshotCount cannot be < 1")
		}
		opts.snapshotCount = count
		return nil
	}
}

func SnapshotCatchUpEntries(count int64) Option {
	return func(opts *options) error {
		if count < 1 {
			return errors.New("snapshotCatchUpEntries cannot be < 1")
		}
		opts.snapshotCatchUpEntries = count
		return nil
	}
}

func (this *options) ClusterId() uint64 {
	if this.clusterName == "" {
		return 0
	}
	return this.hash(this.clusterName)
}

func ClusterName(name string) Option {
	return func(opts *options) error {
		opts.clusterName = name
		return nil
	}
}
