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
	redisHost string
	redisPort uint
	grpcPort  uint
)

func init() {
	flag.UintVar(&grpcPort, "grpcPort", 0, "Port on which Nexus GRPC server listens")
	flag.StringVar(&redisHost, "redisHost", "127.0.0.1", "Redis host")
	flag.UintVar(&redisPort, "redisPort", 6379, "Redis port")
	stopChan = setupSignalNotify()
}

func validateFlags() {
	if grpcPort <= 0 || grpcPort > 65535 {
		log.Panicf("Given GRPC port: %d of Nexus server is invalid", grpcPort)
	}
	if redisPort <= 0 || redisPort > 65535 {
		log.Panicf("Given Redis port: %d is invalid", redisPort)
	}
}

func main() {
	flag.Parse()
	validateFlags()

	if db, err := store.NewRedisDB(redisHost, redisPort); err != nil {
		panic(err)
	} else {
		repl, _ := api.NewRaftReplicator(db, raft.OptionsFromFlags()...)
		ns := grpc.NewNexusService(grpcPort, repl)
		go ns.ListenAndServe()
		sig := <-stopChan
		log.Printf("[WARN] Caught signal: %v. Shutting down...", sig)
		ns.Close()
	}
}
