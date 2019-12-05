package main

import (
	"fmt"

	"github.com/flipkart-incubator/nexus/pkg/api"
	"github.com/flipkart-incubator/nexus/pkg/raft"
)

func main() {
	repl, _ := api.NewRaftReplicator(nil, raft.NodeId(1), raft.LogDir("/tmp/logs"), raft.SnapDir("/tmp/snap"))
	repl.Start()
	repl.Replicate("hello world")
	repl.Stop()
	fmt.Println("Welcome to Nexus !!!")
}
