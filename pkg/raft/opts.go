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

	"github.com/coreos/etcd/raft"
)

type Option func(*options) error

type Options interface {
	NodeId() uint64
	NodeUrl() *url.URL
	Join() bool
	LogDir() string
	SnapDir() string
	ClusterUrls() map[uint64]string
	ReplTimeout() time.Duration
	ReadOption() raft.ReadOnlyOption
	StatsDAddr() string
}

type options struct {
	nodeUrl         *url.URL
	nodeUrlStr      string
	logDir          string
	snapDir         string
	clusterUrl      string
	clusterUrls     []*url.URL
	replTimeout     time.Duration
	leaseBasedReads bool
	statsdAddr      string
}

var (
	opts              options
	replTimeoutInSecs int64
)

func init() {
	flag.StringVar(&opts.nodeUrlStr, "nexusNodeUrl", "", "Url for the Nexus service to be started on this node (format: http://<local_node>:<port_num>)")
	flag.StringVar(&opts.logDir, "nexusLogDir", "/tmp/logs", "Dir for storing RAFT logs")
	flag.StringVar(&opts.snapDir, "nexusSnapDir", "/tmp/snap", "Dir for storing RAFT snapshots")
	flag.StringVar(&opts.clusterUrl, "nexusClusterUrl", "", "Comma separated list of Nexus URLs of other nodes in the cluster")
	flag.Int64Var(&replTimeoutInSecs, "nexusReplTimeout", 5, "Replication timeout in seconds")
	flag.BoolVar(&opts.leaseBasedReads, "nexusLeaseBasedReads", true, "Perform reads using RAFT leader leases")
	flag.StringVar(&opts.statsdAddr, "nexusStatsDAddr", "", "StatsD server address (host:port) for relaying various metrics")
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
	return this.nodeId(this.nodeUrl.Host)
}

func (this *options) nodeId(url string) uint64 {
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
	return fmt.Sprintf("%s/node_%d", this.logDir, this.NodeId())
}

func (this *options) SnapDir() string {
	return fmt.Sprintf("%s/node_%d", this.snapDir, this.NodeId())
}

func (this *options) ClusterUrls() map[uint64]string {
	res := make(map[uint64]string, len(this.clusterUrls))
	for _, nodeUrl := range this.clusterUrls {
		id := this.nodeId(nodeUrl.Host)
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
