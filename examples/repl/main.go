package main

import (
	"bufio"
	"fmt"
	"os"
	str "strings"

	mstore "github.com/flipkart-incubator/nexus/examples/mysql_repl/store"
	rstore "github.com/flipkart-incubator/nexus/examples/redis_repl/store"
	"github.com/flipkart-incubator/nexus/internal/grpc"
)

func printUsage() {
	fmt.Printf("Usage: %s <mysql|redis> <nexus_url>\n", os.Args[0])
}

func newNexusClient(nexus_url string) *grpc.NexusClient {
	if nc, err := grpc.NewInSecureNexusClient(nexus_url); err != nil {
		panic(err)
	} else {
		return nc
	}
}

func sendMySQLCmd(nc *grpc.NexusClient, cmd string) error {
	save_req := &mstore.SaveRequest{StmtTmpl: cmd}
	if bts, err := save_req.ToBytes(); err != nil {
		return err
	} else {
		if err := nc.Replicate(bts); err != nil {
			return err
		} else {
			return nil
		}
	}
}

func sendRedisCmd(nc *grpc.NexusClient, cmd string) error {
	if save_req, err := rstore.NewSaveRequest(cmd); err != nil {
		return err
	} else {
		if bts, err := save_req.ToBytes(); err != nil {
			return err
		} else {
			if err := nc.Replicate(bts); err != nil {
				return err
			} else {
				return nil
			}
		}
	}
}

func replMySQL(nexus_url string) {
	nc := newNexusClient(nexus_url)
	defer nc.Close()

	in := bufio.NewScanner(os.Stdin)
	fmt.Print("mysql> ")
	for in.Scan() {
		cmd := in.Text()
		if err := sendMySQLCmd(nc, str.TrimSpace(cmd)); err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println("OK")
			fmt.Print("mysql> ")
		}
	}
}

func replRedis(nexus_url string) {
	nc := newNexusClient(nexus_url)
	defer nc.Close()

	in := bufio.NewScanner(os.Stdin)
	fmt.Print("redis> ")
	for in.Scan() {
		cmd := in.Text()
		if err := sendRedisCmd(nc, str.TrimSpace(cmd)); err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println("OK")
			fmt.Print("redis> ")
		}
	}
}

func main() {
	if len(os.Args) != 3 {
		printUsage()
		return
	}

	db, nexus_url := str.ToLower(str.TrimSpace(os.Args[1])), str.TrimSpace(os.Args[2])
	switch db {
	case "mysql":
		replMySQL(nexus_url)
	case "redis":
		replRedis(nexus_url)
	default:
		printUsage()
	}
}
