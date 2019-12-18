package api

import (
	context "context"

	internal_raft "github.com/flipkart-incubator/nexus/internal/raft"
	"github.com/flipkart-incubator/nexus/pkg/db"
	"github.com/flipkart-incubator/nexus/pkg/raft"
)

type RaftReplicator interface {
	Start()
	Replicate(context.Context, []byte) error
	Stop()
}

func NewRaftReplicator(store db.Store, opts ...raft.Option) (RaftReplicator, error) {
	return internal_raft.NewReplicator(store, opts...)
}
