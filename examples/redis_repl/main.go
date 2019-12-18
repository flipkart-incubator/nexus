package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/flipkart-incubator/nexus/examples/redis_repl/store"
	"github.com/flipkart-incubator/nexus/pkg/api"
	"github.com/flipkart-incubator/nexus/pkg/raft"
)

func setupSignalNotify() <-chan os.Signal {
	signals := []os.Signal{syscall.SIGINT, syscall.SIGQUIT, syscall.SIGSTOP, syscall.SIGTERM}
	stopChan := make(chan os.Signal, len(signals))
	signal.Notify(stopChan, signals...)
	return stopChan
}

var (
	stopChan   <-chan os.Signal
	nodeId     int
	logDir     string
	snapDir    string
	clusterUrl string
	redisPort  int
	redisDB    int
)

const replTimeout = 5 * time.Second

func init() {
	flag.IntVar(&nodeId, "nodeId", -1, "Node ID (> 0) of the current node")
	flag.StringVar(&logDir, "logDir", "/tmp/logs", "Dir for storing RAFT logs")
	flag.StringVar(&snapDir, "snapDir", "/tmp/snap", "Dir for storing RAFT snapshots")
	flag.StringVar(&clusterUrl, "clusterUrl", "", "Comma separated list of Nexus URLs")
	flag.IntVar(&redisPort, "redisPort", 6379, "Redis port")
	flag.IntVar(&redisDB, "redisDB", 0, "Redis DB Index")
	stopChan = setupSignalNotify()
}

func main() {
	flag.Parse()
	if db, err := store.NewRedisDB(redisPort, redisDB); err != nil {
		panic(err)
	} else {
		repl, _ := api.NewRaftReplicator(db,
			raft.NodeId(nodeId),
			raft.LogDir(logDir),
			raft.SnapDir(snapDir),
			raft.ClusterUrl(clusterUrl),
			raft.ReplicationTimeout(replTimeout),
		)
		repl.Start()
		sig := <-stopChan
		log.Printf("[WARN] Caught signal: %v. Shutting down...", sig)
		repl.Stop()
	}
}
