package models

import (
	"bytes"
	"encoding/gob"
	"github.com/golang/protobuf/proto"
	"github.com/shamaton/msgpack"
	vmsgpack "github.com/vmihailenco/msgpack/v5"
	"math/rand"
	"testing"
)

type internalNexusRequest struct {
	ID  uint64 `msgpack:"i"`
	Req []byte `msgpack:"r"`
}

func BenchmarkMsgPackShamaton(b *testing.B) {
	token := make([]byte, 4096)
	rand.Read(token)

	r := internalNexusRequest{
		ID:  1,
		Req: token,
	}

	for i := 0; i < b.N; i++ {
		msgpack.Marshal(r)
	}
}

func BenchmarkMsgPackVmihailenco(b *testing.B) {
	token := make([]byte, 4096)
	rand.Read(token)

	r := internalNexusRequest{
		ID:  1,
		Req: token,
	}

	for i := 0; i < b.N; i++ {
		vmsgpack.Marshal(r)
	}
}


func BenchmarkProtobuf(b *testing.B) {
	token := make([]byte, 4096)
	rand.Read(token)

	r := NexusInternalRequest{
		ID : 1,
		Req: token,
	}
	for i := 0; i < b.N; i++ {
		proto.Marshal(&r)
	}
}


func BenchmarkGOB(b *testing.B) {
	token := make([]byte, 4096)
	rand.Read(token)

	r := NexusInternalRequest{
		ID : 1,
		Req: token,
	}
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		gob.NewEncoder(&buf).Encode(r)
	}
}

func BenchmarkDecodeMsgPackShamaton(b *testing.B) {
	token := make([]byte, 4096)
	rand.Read(token)

	r := internalNexusRequest{
		ID:  1,
		Req: token,
	}

	bytes, _ := msgpack.Marshal(r)
	for i := 0; i < b.N; i++ {
		msgpack.Unmarshal(bytes, r)
	}
}

func BenchmarkDecodeMsgPackVmihailenco(b *testing.B) {
	token := make([]byte, 4096)
	rand.Read(token)

	r := internalNexusRequest{
		ID:  1,
		Req: token,
	}

	bytes, _ := vmsgpack.Marshal(r)
	for i := 0; i < b.N; i++ {
		vmsgpack.Unmarshal(bytes, r)
	}
}


func BenchmarkDecodeProtobuf(b *testing.B) {
	token := make([]byte, 4096)
	rand.Read(token)

	r := NexusInternalRequest{
		ID : 1,
		Req: token,
	}

	bytes, _ := proto.Marshal(&r)
	for i := 0; i < b.N; i++ {
		proto.Unmarshal(bytes, &r)
	}


}


func BenchmarkDecodeGOB(b *testing.B) {
	token := make([]byte, 4096)
	rand.Read(token)

	r := NexusInternalRequest{
		ID : 1,
		Req: token,
	}

	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(r)

	for i := 0; i < b.N; i++ {
		buf := bytes.NewBuffer(buf.Bytes())
		gob.NewDecoder(buf).Decode(r)
	}
}