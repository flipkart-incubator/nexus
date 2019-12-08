package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

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

var ctr = 0

func save(repl api.RaftReplicator, t time.Time) {
	key := fmt.Sprintf("%d.Key#%d", nodeId, ctr)
	val := fmt.Sprintf("%d", t.UnixNano())

	save_req := &SaveRequest{"SET", key, val}
	if bts, err := save_req.ToBytes(); err != nil {
		fmt.Printf("Error occurred while converting SaveRequest: %s to bytes. Error: %v\n", save_req, err)
	} else {
		if err := repl.Replicate(bts); err != nil {
			fmt.Printf("Error occurred while replicating SaveRequest: %s. Error: %v\n", save_req, err)
		} else {
			fmt.Printf("Successfully replicated SaveRequest: %s", save_req)
			ctr++
		}
	}
}

func main() {
	flag.Parse()
	if db, err := NewRedisDB(redisPort, redisDB); err != nil {
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
		ticker := time.Tick(5 * time.Second)
		for {
			select {
			case t := <-ticker:
				save(repl, t)
			case sig := <-stopChan:
				fmt.Printf("[WARN] Caught signal: %v. Shutting down...", sig)
				repl.Stop()
				break
			}
		}
	}
}
