package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/flipkart-incubator/nexus/examples/redis_repl/store"
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
	stopChan  <-chan os.Signal
	port      uint
	redisPort int
	redisDB   int
)

func init() {
	flag.UintVar(&port, "grpcPort", 0, "Port on which Nexus GRPC server listens")
	flag.IntVar(&redisPort, "redisPort", 6379, "Redis port")
	flag.IntVar(&redisDB, "redisDB", 0, "Redis DB Index")
	stopChan = setupSignalNotify()
}

func main() {
	flag.Parse()
	if db, err := store.NewRedisDB(redisPort, redisDB); err != nil {
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
