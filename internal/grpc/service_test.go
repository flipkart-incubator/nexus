package grpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/flipkart-incubator/nexus/models"
	"hash/fnv"
	"testing"

	"github.com/flipkart-incubator/nexus/pkg/api"
)

const (
	svcPort  = 9827
	svcHost  = "localhost"
	numCases = 10
)

func TestNexusService(t *testing.T) {
	repl := newMockRepl()
	ns := NewNexusService(svcPort, repl)
	defer ns.Close()
	go ns.ListenAndServe()

	svcAddr := fmt.Sprintf("%s:%d", svcHost, svcPort)
	if nc, err := NewInSecureNexusClient(svcAddr); err != nil {
		t.Fatal(err)
	} else {
		defer nc.Close()
		checkHealth(t, nc)
		for i := 1; i <= numCases; i++ {
			data := []byte(fmt.Sprintf("test_%d", i))
			replicate(t, nc, data)
			assertRepl(t, repl, data)
		}
	}
}

func checkHealth(t *testing.T, nc *NexusClient) {
	res := nc.HealthCheck()
	if res != api.HealthCheckResponse_SERVING {
		t.Fatalf("Bad health of Nexus GRPC service. Status: %s", res.String())
	}
}

func replicate(t *testing.T, nc *NexusClient, data []byte) {
	if _, err := nc.Save(data, nil); err != nil {
		t.Fatal(err)
	}
}

func assertRepl(t *testing.T, repl *mockRepl, data []byte) {
	if !repl.hasData(data) {
		t.Errorf("Data not found: %s", data)
	}
}

type mockRepl struct {
	data map[uint32][]byte
}

func newMockRepl() *mockRepl {
	return &mockRepl{data: make(map[uint32][]byte)}
}

func (this *mockRepl) Id() uint64 {
	return 0
}

func (this *mockRepl) Start() {
}

func (this *mockRepl) Stop() {
}

func (this *mockRepl) Load(_ context.Context, data []byte) ([]byte, error) {
	req := new(api.LoadRequest)
	_ = req.Decode(data)
	id := binary.BigEndian.Uint32(req.Data)
	return this.data[id], nil
}

func (this *mockRepl) Save(_ context.Context, data []byte) ([]byte, error) {
	req := new(api.SaveRequest)
	_ = req.Decode(data)
	if hsh, err := hashCode(req.Data); err != nil {
		return nil, err
	} else {
		this.data[hsh] = req.Data
		return nil, nil
	}
}

func (this *mockRepl) AddMember(context.Context, string) error {
	return errors.New("mockRepl::AddMember not implemented")
}

func (this *mockRepl) RemoveMember(context.Context, string) error {
	return errors.New("mockRepl::RemoveMember not implemented")
}

func (this *mockRepl) ListMembers() (uint64, map[uint64]*models.NodeInfo) {
	return uint64(0), nil
}

func (this *mockRepl) hasData(data []byte) bool {
	code, _ := hashCode(data)
	_, present := this.data[code]
	return present
}

func hashCode(data []byte) (uint32, error) {
	hsh := fnv.New32()
	if _, err := hsh.Write(data); err != nil {
		return 0, err
	} else {
		return hsh.Sum32(), nil
	}
}
