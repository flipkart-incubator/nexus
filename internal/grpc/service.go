package grpc

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/flipkart-incubator/nexus/pkg/api"
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

func (this *NexusService) Replicate(ctx context.Context, req *api.ReplicateRequest) (*api.ReplicateResponse, error) {
	if res, err := this.repl.Save(ctx, req.Data); err != nil {
		return &api.ReplicateResponse{Status: &api.Status{Code: -1, Message: err.Error()}, Data: nil}, err
	} else {
		return &api.ReplicateResponse{Status: &api.Status{}, Data: res}, nil
	}
}

func (this *NexusService) AddNode(ctx context.Context, req *api.AddNodeRequest) (*api.Status, error) {
	if err := this.repl.AddMember(ctx, int(req.NodeId), req.NodeUrl); err != nil {
		return &api.Status{Code: -1, Message: err.Error()}, err
	}
	return &api.Status{}, nil
}

func (this *NexusService) RemoveNode(ctx context.Context, req *api.RemoveNodeRequest) (*api.Status, error) {
	if err := this.repl.RemoveMember(ctx, int(req.NodeId)); err != nil {
		return &api.Status{Code: -1, Message: err.Error()}, err
	}
	return &api.Status{}, nil
}
