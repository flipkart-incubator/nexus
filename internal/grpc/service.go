package grpc

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/flipkart-incubator/nexus/pkg/api"
	"github.com/golang/protobuf/ptypes/empty"
	ggrpc "google.golang.org/grpc"
)

type NexusService struct {
	port uint
	repl api.RaftReplicator
}

func NewNexusService(port uint, repl api.RaftReplicator) *NexusService {
	return &NexusService{port, repl}
}

func (this *NexusService) ListenAndServe() {
	this.repl.Start()
	this.NewGRPCServer().Serve(this.NewListener())
}

func (this *NexusService) NewGRPCServer() *ggrpc.Server {
	grpcServer := ggrpc.NewServer()
	api.RegisterNexusServer(grpcServer, this)
	return grpcServer
}

func (this *NexusService) NewListener() net.Listener {
	if lis, err := net.Listen("tcp", fmt.Sprintf(":%d", this.port)); err != nil {
		log.Fatalf("failed to listen: %v", err)
		return nil
	} else {
		return lis
	}
}

func (this *NexusService) Close() error {
	this.repl.Stop()
	return nil
}

func (this *NexusService) Check(ctx context.Context, req *api.HealthCheckRequest) (*api.HealthCheckResponse, error) {
	return &api.HealthCheckResponse{Status: api.HealthCheckResponse_SERVING}, nil
}

func (this *NexusService) Save(ctx context.Context, req *api.SaveRequest) (*api.SaveResponse, error) {
	if replReq, err := req.Encode(); err != nil {
		return nil, err
	} else {
		if res, err := this.repl.Save(ctx, replReq); err != nil {
			return &api.SaveResponse{Status: &api.Status{Code: -1, Message: err.Error()}, ReqData: req.Data}, err
		} else {
			return &api.SaveResponse{Status: &api.Status{}, ReqData: req.Data, ResData: res}, nil
		}
	}
}

func (this *NexusService) Load(ctx context.Context, req *api.LoadRequest) (*api.LoadResponse, error) {
	if replReq, err := req.Encode(); err != nil {
		return nil, err
	} else {
		if res, err := this.repl.Load(ctx, replReq); err != nil {
			return &api.LoadResponse{Status: &api.Status{Code: -1, Message: err.Error()}, ReqData: req.Data}, err
		} else {
			return &api.LoadResponse{Status: &api.Status{}, ReqData: req.Data, ResData: res}, nil
		}
	}
}

func (this *NexusService) AddNode(ctx context.Context, req *api.AddNodeRequest) (*api.Status, error) {
	if err := this.repl.AddMember(ctx, req.NodeUrl); err != nil {
		return &api.Status{Code: -1, Message: err.Error()}, err
	}
	return &api.Status{}, nil
}

func (this *NexusService) RemoveNode(ctx context.Context, req *api.RemoveNodeRequest) (*api.Status, error) {
	if err := this.repl.RemoveMember(ctx, req.NodeUrl); err != nil {
		return &api.Status{Code: -1, Message: err.Error()}, err
	}
	return &api.Status{}, nil
}

func (this *NexusService) ListNodes(ctx context.Context, _ *empty.Empty) (*api.ListNodesResponse, error) {
	ldr, clusNodes := this.repl.ListMembers()
	return &api.ListNodesResponse{Status: &api.Status{}, Leader: ldr, Nodes: clusNodes}, nil
}
