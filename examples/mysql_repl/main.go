package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

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

var (
	stopChan     <-chan os.Signal
	port         uint
	mysqlConnUrl string
)

func init() {
	flag.UintVar(&port, "grpcPort", 0, "Port on which Nexus GRPC server listens")
	flag.StringVar(&mysqlConnUrl, "mysqlConnUrl", "", "MySQL connection string [username:password@tcp(mysqlhost:mysqlport)/dbname?tls=skip-verify&autocommit=false]")
	stopChan = setupSignalNotify()
}

func main() {
	flag.Parse()
	if db, err := store.NewMySQLDB(mysqlConnUrl); err != nil {
		panic(err)
	} else {
		repl, _ := api.NewRaftReplicator(db, raft.OptionsFromFlags()...)
		ns := grpc.NewNexusService(port, repl)
		go ns.ListenAndServe()
		sig := <-stopChan
		log.Printf("[WARN] Caught signal: %v. Shutting down...", sig)
		ns.Close()
	}
}
