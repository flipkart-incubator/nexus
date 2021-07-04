package api

import (
	"bytes"
	context "context"
	"encoding/gob"
	"errors"

	internal_raft "github.com/flipkart-incubator/nexus/internal/raft"
	"github.com/flipkart-incubator/nexus/pkg/db"
	"github.com/flipkart-incubator/nexus/pkg/raft"
)

type RaftReplicator interface {
	Start()
	Id() uint64
	Save(context.Context, []byte) ([]byte, error)
	Load(context.Context, []byte) ([]byte, error)
	AddMember(context.Context, string) error
	RemoveMember(context.Context, string) error
	ListMembers() (uint64, map[uint64]string)
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

const RecordSeparator = byte(0x1E)

func (req *SaveRequest) Encode() ([]byte, error) {
	var argBuf bytes.Buffer
	if err := gob.NewEncoder(&argBuf).Encode(req.Args); err != nil {
		return nil, err
	}
	res := bytes.NewBuffer(req.Data)
	res.WriteByte(RecordSeparator)
	_, _ = res.ReadFrom(&argBuf)
	return res.Bytes(), nil
}

func (req *SaveRequest) Decode(data []byte) error {
	sepIndex := bytes.IndexByte(data, RecordSeparator)
	if sepIndex < 0 {
		return errors.New("invalid SaveRequest to decode, no record separator found")
	}

	req.Data = data[0:sepIndex]
	req.Args = make(map[string][]byte)
	buf := bytes.NewBuffer(data[sepIndex+1:])
	err := gob.NewDecoder(buf).Decode(&req.Args)
	return err
}

func (req *LoadRequest) Encode() ([]byte, error) {
	var argBuf bytes.Buffer
	if err := gob.NewEncoder(&argBuf).Encode(req.Args); err != nil {
		return nil, err
	}
	res := bytes.NewBuffer(req.Data)
	res.WriteByte(RecordSeparator)
	_, _ = res.ReadFrom(&argBuf)
	return res.Bytes(), nil
}

func (req *LoadRequest) Decode(data []byte) error {
	sepIndex := bytes.IndexByte(data, RecordSeparator)
	if sepIndex < 0 {
		return errors.New("invalid LoadRequest to decode, no record separator found")
	}

	req.Data = data[0:sepIndex]
	req.Args = make(map[string][]byte)
	buf := bytes.NewBuffer(data[sepIndex+1:])
	err := gob.NewDecoder(buf).Decode(&req.Args)
	return err
}
