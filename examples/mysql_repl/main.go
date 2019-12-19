package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/flipkart-incubator/nexus/examples/mysql_repl/store"
	"github.com/flipkart-incubator/nexus/internal/grpc"
	"github.com/flipkart-incubator/nexus/pkg/api"
	"github.com/flipkart-incubator/nexus/pkg/raft"
)

func setupSignalNotify() <-chan os.Signal {
	signals := []os.Signal{syscall.SIGINT, syscall.SIGQUIT, syscall.SIGSTOP, syscall.SIGTERM}
	stopChan := make(chan os.Signal, len(signals))
	signal.Notify(stopChan, signals...)
	return stopChan
}

const replTimeout = 5 * time.Second

var (
	stopChan     <-chan os.Signal
	nodeId       int
	port         uint
	logDir       string
	snapDir      string
	clusterUrl   string
	mysqlConnUrl string
)

func init() {
	flag.IntVar(&nodeId, "nodeId", -1, "Node ID (> 0) of the current node")
	flag.UintVar(&port, "grpcPort", 0, "Port on which Nexus GRPC server listens")
	flag.StringVar(&logDir, "logDir", "/tmp/logs", "Dir for storing RAFT logs")
	flag.StringVar(&snapDir, "snapDir", "/tmp/snap", "Dir for storing RAFT snapshots")
	flag.StringVar(&clusterUrl, "clusterUrl", "", "Comma separated list of Nexus URLs")
	flag.StringVar(&mysqlConnUrl, "mysqlConnUrl", "", "MySQL connection string [username:password@tcp(mysqlhost:mysqlport)/dbname?tls=skip-verify&autocommit=false]")
	stopChan = setupSignalNotify()
}

func main() {
	flag.Parse()
	if db, err := store.NewMySQLDB(mysqlConnUrl); err != nil {
		panic(err)
	} else {
		repl, _ := api.NewRaftReplicator(db,
			raft.NodeId(nodeId),
			raft.LogDir(logDir),
			raft.SnapDir(snapDir),
			raft.ClusterUrl(clusterUrl),
			raft.ReplicationTimeout(replTimeout),
		)
		ns := grpc.NewNexusService(port, repl)
		go ns.ListenAndServe()
		sig := <-stopChan
		log.Printf("[WARN] Caught signal: %v. Shutting down...", sig)
		ns.Close()
	}
}
