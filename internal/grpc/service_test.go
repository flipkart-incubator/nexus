package grpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"testing"
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

	svc_addr := fmt.Sprintf("%s:%d", svcHost, svcPort)
	if nc, err := NewInSecureNexusClient(svc_addr); err != nil {
		t.Fatal(err)
	} else {
		defer nc.Close()
		for i := 1; i <= numCases; i++ {
			data := []byte(fmt.Sprintf("test_%d", i))
			replicate(t, nc, data)
			assertRepl(t, repl, data)
		}
	}
}

func replicate(t *testing.T, nc *NexusClient, data []byte) {
	if _, err := nc.Save(data); err != nil {
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

func (this *mockRepl) Start() {
}

func (this *mockRepl) Stop() {
}

func (this *mockRepl) Load(ctx context.Context, data []byte) ([]byte, error) {
	id := binary.BigEndian.Uint32(data)
	return this.data[id], nil
}

func (this *mockRepl) Save(ctx context.Context, data []byte) ([]byte, error) {
	if hsh, err := hashCode(data); err != nil {
		return nil, err
	} else {
		this.data[hsh] = data
		return nil, nil
	}
}

func (this *mockRepl) AddMember(ctx context.Context, nodeId int, nodeUrl string) error {
	return errors.New("mockRepl::AddMember not implemented")
}

func (this *mockRepl) RemoveMember(ctx context.Context, nodeId int) error {
	return errors.New("mockRepl::RemoveMember not implemented")
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
