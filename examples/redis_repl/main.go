package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/flipkart-incubator/nexus/examples/redis_repl/store"
	"github.com/flipkart-incubator/nexus/internal/grpc"
	"github.com/flipkart-incubator/nexus/internal/stats"
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
	redisHost  string
	redisPort  uint
	metadataDB uint
	grpcPort   uint
	statsdAddr string
)

func init() {
	flag.UintVar(&grpcPort, "grpcPort", 0, "Port on which Nexus GRPC server listens")
	flag.StringVar(&redisHost, "redisHost", "127.0.0.1", "Redis host")
	flag.UintVar(&redisPort, "redisPort", 6379, "Redis port")
	flag.UintVar(&metadataDB, "metadataDB", 13, "DB index for storing RAFT metadata")
	flag.StringVar(&statsdAddr, "statsdAddr", "", "StatsD service address in host:port format")
	stopChan = setupSignalNotify()
}

func validateFlags() {
	if grpcPort <= 0 || grpcPort > 65535 {
		log.Panicf("Given GRPC port: %d of Nexus server is invalid", grpcPort)
	}
	if redisPort <= 0 || redisPort > 65535 {
		log.Panicf("Given Redis port: %d is invalid", redisPort)
	}
	if statsdAddr != "" && strings.IndexRune(statsdAddr, ':') < 0 {
		log.Panicf("Given StatsD address: %s is invalid. Must be in host:port format.", statsdAddr)
	}
}

func initStatsD() stats.Client {
	if statsdAddr != "" {
		return stats.NewStatsDClient(statsdAddr, "nexus_redis")
	}
	return stats.NewNoOpClient()
}

func main() {
	flag.Parse()
	validateFlags()
	statsd := initStatsD()

	if db, err := store.NewRedisDB(redisHost, redisPort, metadataDB, statsd); err != nil {
		panic(err)
	} else {
		nexusOpts := raft.OptionsFromFlags()
		nexusOpts = append(nexusOpts, raft.StatsDAddr(statsdAddr))
		if repl, err := api.NewRaftReplicator(db, nexusOpts...); err != nil {
			panic(err)
		} else {
			ns := grpc.NewNexusService(grpcPort, repl)
			go ns.ListenAndServe()
			sig := <-stopChan
			log.Printf("[WARN] Caught signal: %v. Shutting down...", sig)
			_ = ns.Close()
		}
	}
}
