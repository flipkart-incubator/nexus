package grpc

import (
	"context"
	"errors"
	"github.com/flipkart-incubator/nexus/models"
	"time"

	"github.com/flipkart-incubator/nexus/pkg/api"
	"github.com/golang/protobuf/ptypes/empty"
	ggrpc "google.golang.org/grpc"
)

const (
	ReadBufSize  = 10 << 20
	WriteBufSize = 10 << 20
	Timeout      = 10 * time.Second
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

func (this *NexusClient) HealthCheck() api.HealthCheckResponse_ServingStatus {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	req := &api.HealthCheckRequest{Service: ""}
	if res, err := this.nexusCli.Check(ctx, req); err != nil {
		return api.HealthCheckResponse_NOT_SERVING
	} else {
		return res.Status
	}
}

func (this *NexusClient) Save(data []byte, params map[string][]byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	saveReq := &api.SaveRequest{Data: data, Args: params}
	if res, err := this.nexusCli.Save(ctx, saveReq); err != nil {
		return nil, err
	} else {
		if res.Status.Code != 0 {
			return nil, errors.New(res.Status.Message)
		} else {
			return res.ResData, nil
		}
	}
}

func (this *NexusClient) Load(data []byte, params map[string][]byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	loadReq := &api.LoadRequest{Data: data, Args: params}
	if res, err := this.nexusCli.Load(ctx, loadReq); err != nil {
		return nil, err
	} else {
		if res.Status.Code != 0 {
			return nil, errors.New(res.Status.Message)
		} else {
			return res.ResData, nil
		}
	}
}

func (this *NexusClient) AddNode(nodeUrl string) error {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	req := &api.AddNodeRequest{NodeUrl: nodeUrl}
	if res, err := this.nexusCli.AddNode(ctx, req); err != nil {
		return err
	} else if res.Code != 0 {
		return errors.New(res.Message)
	}
	return nil
}

func (this *NexusClient) RemoveNode(nodeUrl string) error {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	req := &api.RemoveNodeRequest{NodeUrl: nodeUrl}
	if res, err := this.nexusCli.RemoveNode(ctx, req); err != nil {
		return err
	} else if res.Code != 0 {
		return errors.New(res.Message)
	}
	return nil
}

func (this *NexusClient) ListNodes() (uint64, map[uint64]*models.NodeInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	res, _ := this.nexusCli.ListNodes(ctx, &empty.Empty{})
	return res.Leader, res.Nodes
}

func (this *NexusClient) Close() error {
	return this.cliConn.Close()
}
