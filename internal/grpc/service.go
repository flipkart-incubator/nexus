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
	if err := this.repl.Replicate(ctx, req.Data); err != nil {
		return &api.ReplicateResponse{Code: -1, Message: err.Error()}, err
	} else {
		return &api.ReplicateResponse{Code: 0}, nil
	}
}
