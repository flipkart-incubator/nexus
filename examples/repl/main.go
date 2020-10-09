package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	str "strings"

	mstore "github.com/flipkart-incubator/nexus/examples/mysql_repl/store"
	"github.com/flipkart-incubator/nexus/internal/grpc"
)

func printUsage() {
	fmt.Printf("Usage: %s <nexus_url> <command> [<options>]\n"+
		"Following commands are supported:\n"+
		"listNodes\n"+
		"addNode <nodeId> <nodeAddr>\n"+
		"removeNode <nodeId>\n"+
		"<mysql|redis> load <expression>\n"+
		"<mysql|redis> save <expression>\n", os.Args[0])
}

func newNexusClient(nexus_url string) *grpc.NexusClient {
	if nc, err := grpc.NewInSecureNexusClient(nexus_url); err != nil {
		panic(err)
	} else {
		return nc
	}
}

func loadMySQLCmd(nc *grpc.NexusClient, cmd string) ([]byte, error) {
	load_req := &mstore.LoadRequest{StmtTmpl: cmd}
	if bts, err := load_req.ToBytes(); err != nil {
		return nil, err
	} else {
		return nc.Load(bts)
	}
}

func saveMySQLCmd(nc *grpc.NexusClient, cmd string) ([]byte, error) {
	save_req := &mstore.SaveRequest{StmtTmpl: cmd}
	if bts, err := save_req.ToBytes(); err != nil {
		return nil, err
	} else {
		return nc.Save(bts)
	}
}

func sendRedisCmd(nc *grpc.NexusClient, mode, cmd string) ([]byte, error) {
	switch str.ToLower(mode) {
	case "load":
		return nc.Load([]byte(cmd))
	case "save":
		return nc.Save([]byte(cmd))
	}
	return nil, errors.New("Unknown mode: " + mode)
}

func sendMySQL(nexus_url string, mode, cmd string) {
	nc := newNexusClient(nexus_url)
	defer nc.Close()

	switch str.ToLower(str.TrimSpace(mode)) {
	case "load":
		if res, err := loadMySQLCmd(nc, str.TrimSpace(cmd)); err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Printf("Response from MySQL (without quotes): '%s'\n", res)
		}
	case "save":
		if res, err := saveMySQLCmd(nc, str.TrimSpace(cmd)); err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Printf("Response from MySQL (without quotes): '%s'\n", res)
		}
	default:
		fmt.Printf("Unknown mode: " + mode)
	}

}

func sendRedis(nexus_url string, mode, cmd string) {
	nc := newNexusClient(nexus_url)
	defer nc.Close()

	if res, err := sendRedisCmd(nc, str.TrimSpace(mode), str.TrimSpace(cmd)); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("Response from Redis (without quotes): '%s'\n", res)
	}
}

func listNodes(nexus_url string) {
	nc := newNexusClient(nexus_url)
	defer nc.Close()

	listNodesUsingCli(nc)
}

func listNodesUsingCli(nc *grpc.NexusClient) {
	fmt.Println("Current cluster members:")
	for nodeId, nodeUrl := range nc.ListNodes() {
		fmt.Printf("%d => %s\n", nodeId, nodeUrl)
	}
}

func addNode(nexus_url string, args []string) {
	if len(args) < 2 {
		fmt.Println("Error: Both <nodeId> and <nodeAddr> must be provided")
		printUsage()
		return
	}
	node_id, node_addr := strings.TrimSpace(args[0]), strings.TrimSpace(args[1])
	nodeId, err := strconv.ParseUint(node_id, 10, 32)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	nc := newNexusClient(nexus_url)
	defer nc.Close()

	err = nc.AddNode(uint32(nodeId), node_addr)
	if err != nil {
		fmt.Println(err.Error())
	}
	listNodesUsingCli(nc)
}

func removeNode(nexus_url string, args []string) {
	if len(args) < 1 {
		fmt.Println("Error: Missing <nodeId>")
		printUsage()
		return
	}
	node_id := strings.TrimSpace(args[0])
	nodeId, err := strconv.ParseUint(node_id, 10, 32)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	nc := newNexusClient(nexus_url)
	defer nc.Close()

	err = nc.RemoveNode(uint32(nodeId))
	if err != nil {
		fmt.Println(err.Error())
	}
	listNodesUsingCli(nc)
}

func main() {
	arg_len := len(os.Args)
	if arg_len < 3 {
		printUsage()
		return
	}

	nexus_url, cmd := str.TrimSpace(os.Args[1]), str.ToLower(str.TrimSpace(os.Args[2]))
	switch cmd {
	case "listnodes":
		listNodes(nexus_url)
	case "addnode":
		addNode(nexus_url, os.Args[3:])
	case "removenode":
		removeNode(nexus_url, os.Args[3:])
	case "mysql":
		sendMySQL(nexus_url, os.Args[3], os.Args[4])
	case "redis":
		sendRedis(nexus_url, os.Args[3], os.Args[4])
	default:
		printUsage()
	}
}
