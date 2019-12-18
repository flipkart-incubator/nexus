package grpc

import (
	"context"
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
	if err := nc.Replicate(data); err != nil {
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

func (this *mockRepl) Replicate(ctx context.Context, data []byte) error {
	if hsh, err := hashCode(data); err != nil {
		return err
	} else {
		this.data[hsh] = data
		return nil
	}
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
