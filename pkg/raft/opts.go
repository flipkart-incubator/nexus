package raft

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/coreos/etcd/raft"
)

type Option func(*options) error

type Options interface {
	NodeId() uint64
	ListenAddr() string
	Join() bool
	LogDir() string
	SnapDir() string
	ClusterUrls() map[uint64]string
	ReplTimeout() time.Duration
	ReadOption() raft.ReadOnlyOption
	StatsDAddr() string
}

type options struct {
	listenAddr      string
	logDir          string
	snapDir         string
	clusterUrl      string
	replTimeout     time.Duration
	leaseBasedReads bool
	statsdAddr      string
}

var (
	opts              options
	replTimeoutInSecs int64
)

func init() {
	flag.StringVar(&opts.listenAddr, "nexusListenAddr", "", "Address on which Nexus service binds")
	flag.StringVar(&opts.logDir, "nexusLogDir", "/tmp/logs", "Dir for storing RAFT logs")
	flag.StringVar(&opts.snapDir, "nexusSnapDir", "/tmp/snap", "Dir for storing RAFT snapshots")
	flag.StringVar(&opts.clusterUrl, "nexusClusterUrl", "", "Comma separated list of Nexus URLs")
	flag.Int64Var(&replTimeoutInSecs, "nexusReplTimeout", 5, "Replication timeout in seconds")
	flag.BoolVar(&opts.leaseBasedReads, "nexusLeaseBasedReads", true, "Perform reads using RAFT leader leases")
	flag.StringVar(&opts.statsdAddr, "nexusStatsDAddr", "", "StatsD server address (host:port) for relaying various metrics")
}

func OptionsFromFlags() []Option {
	return []Option{
		ListenAddr(opts.listenAddr),
		LogDir(opts.logDir),
		SnapDir(opts.snapDir),
		ClusterUrl(opts.clusterUrl),
		ReplicationTimeout(time.Duration(replTimeoutInSecs) * time.Second),
		LeaseBasedReads(opts.leaseBasedReads),
		StatsDAddr(opts.statsdAddr),
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
	return this.nodeId(this.listenAddr)
}

func (this *options) nodeId(addr string) uint64 {
	hash := sha1.Sum([]byte(addr))
	return binary.BigEndian.Uint64(hash[:8])
}

func (this *options) ListenAddr() string {
	return this.listenAddr
}

func (this *options) Join() bool {
	clusUrls := this.ClusterUrls()
	for _, clusUrl := range clusUrls {
		if strings.TrimSpace(clusUrl) == this.listenAddr {
			return false
		}
	}
	return true
}

func (this *options) LogDir() string {
	return fmt.Sprintf("%s/node_%d", this.logDir, this.NodeId())
}

func (this *options) SnapDir() string {
	return fmt.Sprintf("%s/node_%d", this.snapDir, this.NodeId())
}

func (this *options) ClusterUrls() map[uint64]string {
	nodes := strings.Split(this.clusterUrl, ",")
	res := make(map[uint64]string, len(nodes))
	for _, node := range nodes {
		id := this.nodeId(node)
		res[id] = node
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

func ListenAddr(addr string) Option {
	return func(opts *options) error {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			return errors.New("Nexus listen address must be given")
		}
		opts.listenAddr = addr
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
