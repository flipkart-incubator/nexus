package main

import (
	"bufio"
	"fmt"
	"os"
	str "strings"

	mstore "github.com/flipkart-incubator/nexus/examples/mysql_repl/store"
	"github.com/flipkart-incubator/nexus/internal/grpc"
)

func printUsage() {
	fmt.Printf("Usage: %s <mysql|redis> <nexus_url> [<expression>]\n", os.Args[0])
}

func newNexusClient(nexus_url string) *grpc.NexusClient {
	if nc, err := grpc.NewInSecureNexusClient(nexus_url); err != nil {
		panic(err)
	} else {
		return nc
	}
}

func sendMySQLCmd(nc *grpc.NexusClient, cmd string) ([]byte, error) {
	save_req := &mstore.SaveRequest{StmtTmpl: cmd}
	if bts, err := save_req.ToBytes(); err != nil {
		return nil, err
	} else {
		return nc.Replicate(bts)
	}
}

func sendRedisCmd(nc *grpc.NexusClient, cmd string) ([]byte, error) {
	return nc.Replicate([]byte(cmd))
}

func sendMySQL(nexus_url string, cmd string) {
	nc := newNexusClient(nexus_url)
	defer nc.Close()

	if res, err := sendMySQLCmd(nc, str.TrimSpace(cmd)); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("Response from MySQL (without quotes): '%s'\n", res)
	}
}

func replMySQL(nexus_url string) {
	nc := newNexusClient(nexus_url)
	defer nc.Close()

	in := bufio.NewScanner(os.Stdin)
	fmt.Print("mysql> ")
	for in.Scan() {
		cmd := in.Text()
		if res, err := sendMySQLCmd(nc, str.TrimSpace(cmd)); err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Printf("Response from MySQL (without quotes): '%s'\n", res)
		}
		fmt.Print("mysql> ")
	}
}

func sendRedis(nexus_url string, cmd string) {
	nc := newNexusClient(nexus_url)
	defer nc.Close()

	if res, err := sendRedisCmd(nc, str.TrimSpace(cmd)); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("Response from Redis (without quotes): '%s'\n", res)
	}
}

func replRedis(nexus_url string) {
	nc := newNexusClient(nexus_url)
	defer nc.Close()

	in := bufio.NewScanner(os.Stdin)
	fmt.Print("redis> ")
	for in.Scan() {
		cmd := in.Text()
		if res, err := sendRedisCmd(nc, str.TrimSpace(cmd)); err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Printf("Response from Redis (without quotes): '%s'\n", res)
		}
		fmt.Print("redis> ")
	}
}

func main() {
	arg_len := len(os.Args)
	if arg_len < 3 || arg_len > 4 {
		printUsage()
		return
	}

	db, nexus_url, repl_mode := str.ToLower(str.TrimSpace(os.Args[1])), str.TrimSpace(os.Args[2]), arg_len == 3
	switch db {
	case "mysql":
		if repl_mode {
			replMySQL(nexus_url)
		} else {
			sendMySQL(nexus_url, os.Args[3])
		}
	case "redis":
		if repl_mode {
			replRedis(nexus_url)
		} else {
			sendRedis(nexus_url, os.Args[3])
		}
	default:
		printUsage()
	}
}
