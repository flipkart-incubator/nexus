package grpc

import (
	"context"
	"errors"
	"time"

	"github.com/flipkart-incubator/nexus/pkg/api"
	ggrpc "google.golang.org/grpc"
)

const (
	ReadBufSize  = 10 << 30
	WriteBufSize = 10 << 30
	Timeout      = 1 * time.Second
)

type NexusClient struct {
	cliConn  *ggrpc.ClientConn
	nexusCli api.NexusClient
}

func NewInSecureNexusClient(svcAddr string) (*NexusClient, error) {
	if conn, err := ggrpc.Dial(svcAddr, ggrpc.WithInsecure(), ggrpc.WithBlock(), ggrpc.WithReadBufferSize(ReadBufSize), ggrpc.WithWriteBufferSize(WriteBufSize)); err != nil {
		return nil, err
	} else {
		nexus_cli := api.NewNexusClient(conn)
		return &NexusClient{conn, nexus_cli}, nil
	}
}

func (this *NexusClient) Replicate(data []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	putReq := &api.ReplicateRequest{Data: data}
	if res, err := this.nexusCli.Replicate(ctx, putReq); err != nil {
		return nil, err
	} else {
		if res.Code != 0 {
			return nil, errors.New(res.Message)
		} else {
			return res.Data, nil
		}
	}
}

func (this *NexusClient) Close() error {
	return this.cliConn.Close()
}
