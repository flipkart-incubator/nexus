package main

import (
	"fmt"
	"github.com/flipkart-incubator/nexus/examples/redis_repl/store"
	"github.com/flipkart-incubator/nexus/internal/grpc"
	"testing"
)

var (
	num        = 1000000
	serviceUrl = "127.0.0.1:9121"
	loremIpsum = "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
)

func BenchmarkRedisWrite(b *testing.B) {
	var nc *grpc.NexusClient
	var err error
	if nc, err = grpc.NewInSecureNexusClient(serviceUrl); err != nil {
		b.Fatal(err)
	}
	b.Logf("Connected to Nexus")
	defer nc.Close()
	for i := 0; i < b.N; i++ {
		//bin/repl 127.0.0.1:9121 redis save db.index=2 "return redis.call('set', 'hello', 'world')" #write
		paramsMap := map[string][]byte{
			store.DBIndexKey: []byte("0"),
		}
		saveCommand := fmt.Sprintf("return redis.call('set', 'hello-%d', '%s')", i, loremIpsum)
		if res, err := nc.Save([]byte(saveCommand), paramsMap); err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Printf("Response from Redis (without quotes): '%s'\n", res)
		}
	}
}
