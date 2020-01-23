package raft

import (
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"
)

type Option func(*options) error

type Options interface {
	NodeId() int
	Join() bool
	LogDir() string
	SnapDir() string
	ClusterUrls() []string
	ReplTimeout() time.Duration
}

type options struct {
	nodeId      int
	join        bool
	logDir      string
	snapDir     string
	clusterUrl  string
	replTimeout time.Duration
}

var (
	opts              options
	replTimeoutInSecs int64
)

func init() {
	flag.IntVar(&opts.nodeId, "nexusNodeId", -1, "Node ID (> 0) of the current node")
	flag.BoolVar(&opts.join, "nexusJoin", false, "Join an existing Nexus cluster (default false)")
	flag.StringVar(&opts.logDir, "nexusLogDir", "/tmp/logs", "Dir for storing RAFT logs")
	flag.StringVar(&opts.snapDir, "nexusSnapDir", "/tmp/snap", "Dir for storing RAFT snapshots")
	flag.StringVar(&opts.clusterUrl, "nexusClusterUrl", "", "Comma separated list of Nexus URLs")
	flag.Int64Var(&replTimeoutInSecs, "nexusReplTimeout", 5, "Replication timeout in seconds")
}

func OptionsFromFlags() []Option {
	return []Option{
		NodeId(opts.nodeId),
		Join(opts.join),
		LogDir(opts.logDir),
		SnapDir(opts.snapDir),
		ClusterUrl(opts.clusterUrl),
		ReplicationTimeout(time.Duration(replTimeoutInSecs) * time.Second),
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

func (this *options) NodeId() int {
	return this.nodeId
}

func (this *options) Join() bool {
	return this.join
}

func (this *options) LogDir() string {
	return fmt.Sprintf("%s/node_%d", this.logDir, this.nodeId)
}

func (this *options) SnapDir() string {
	return fmt.Sprintf("%s/node_%d", this.snapDir, this.nodeId)
}

func (this *options) ClusterUrls() []string {
	return strings.Split(this.clusterUrl, ",")
}

func (this *options) ReplTimeout() time.Duration {
	return this.replTimeout
}

func NodeId(id int) Option {
	return func(opts *options) error {
		if id <= 0 {
			return errors.New("NodeID must be strictly greater than 0")
		}
		opts.nodeId = id
		return nil
	}
}

func Join(join bool) Option {
	return func(opts *options) error {
		opts.join = join
		return nil
	}
}

func LogDir(dir string) Option {
	return func(opts *options) error {
		if dir == "" || strings.TrimSpace(dir) == "" {
			return errors.New("Raft log dir must not be empty")
		}
		opts.logDir = dir
		return nil
	}
}

func SnapDir(dir string) Option {
	return func(opts *options) error {
		if dir == "" || strings.TrimSpace(dir) == "" {
			return errors.New("Raft snapshot dir must not be empty")
		}
		opts.snapDir = dir
		return nil
	}
}

func ClusterUrl(url string) Option {
	return func(opts *options) error {
		if url == "" || strings.TrimSpace(url) == "" {
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
