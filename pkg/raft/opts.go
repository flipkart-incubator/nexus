package raft

import (
	"errors"
	"fmt"
	"strings"
)

type Option func(*options) error

type Options interface {
	NodeId() int
	LogDir() string
	SnapDir() string
	ClusterUrls() []string
}

type options struct {
	nodeId     int
	logDir     string
	snapDir    string
	clusterUrl string
}

func (this *options) NodeId() int {
	return this.nodeId
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

func NewOptions(opts ...Option) (Options, error) {
	options := &options{}
	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}
	return options, nil
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
