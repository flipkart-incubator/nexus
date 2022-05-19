package api

import (
	"context"
	"errors"
	internal_raft "github.com/flipkart-incubator/nexus/internal/raft"
	"github.com/flipkart-incubator/nexus/models"
	"github.com/flipkart-incubator/nexus/pkg/db"
	"github.com/flipkart-incubator/nexus/pkg/raft"
	"github.com/golang/protobuf/proto"
)

type RaftReplicator interface {
	Start()
	Id() uint64
	Save(context.Context, []byte) ([]byte, error)
	Load(context.Context, []byte) ([]byte, error)
	AddMember(context.Context, string) error
	RemoveMember(context.Context, string) error
	ListMembers() (uint64, map[uint64]*models.NodeInfo)
	Stop()
}

func NewRaftReplicator(store db.Store, opts ...raft.Option) (RaftReplicator, error) {
	if store == nil {
		return nil, errors.New("Store must be given")
	}
	if options, err := raft.NewOptions(opts...); err != nil {
		return nil, err
	} else {
		return internal_raft.NewReplicator(store, options), nil
	}
}

func (req *SaveRequest) Encode() ([]byte, error) {
	return proto.Marshal(req)
}

func (req *SaveRequest) Decode(data []byte) error {
	return proto.Unmarshal(data, req)
}

func (req *LoadRequest) Encode() ([]byte, error) {
	return proto.Marshal(req)
}

func (req *LoadRequest) Decode(data []byte) error {
	return proto.Unmarshal(data, req)
}
