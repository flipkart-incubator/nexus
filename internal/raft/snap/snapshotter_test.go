// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	pioutil "github.com/coreos/etcd/pkg/ioutil"
	"github.com/coreos/etcd/pkg/pbutil"
	internal_snap "github.com/coreos/etcd/snap"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/coreos/etcd/raft/raftpb"
)

var testSnap = &raftpb.Snapshot{
	Data: []byte("some snapshot"),
	Metadata: raftpb.SnapshotMetadata{
		ConfState: raftpb.ConfState{
			Nodes: []uint64{1, 2, 3},
		},
		Index: 1,
		Term:  1,
	},
}

func TestSaveAndLoad(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "snapshot")
	err := os.Mkdir(dir, 0700)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	ss := New(dir)
	err = ss.SaveSnapshot(*testSnap, bytes.NewReader(testSnap.Data))
	if err != nil {
		t.Fatal(err)
	}

	g, data, err := ss.LoadSnapshot()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	defer data.Close()
	g.Data, _ = ioutil.ReadAll(data)
	if !reflect.DeepEqual(g, testSnap) {
		t.Errorf("snap = %#v, want %#v", g, testSnap)
	}
}

func TestSaveAndLoadSnapDB(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "snapshot-db")
	err := os.Mkdir(dir, 0700)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	ssi := internal_snap.New(dir)
	ss := New(dir)

	msg := new(internal_snap.Message)
	msg.Message = raftpb.Message{
		Type:     raftpb.MsgSnap,
		To:       1234,
		Snapshot: *testSnap,
	}
	msg.ReadCloser = ioutil.NopCloser(bytes.NewReader(testSnap.Data))
	msg.TotalSize = int64(len(testSnap.Data))
	body := createSnapBody(t, msg)
	_, err = ssi.SaveDBFrom(body, testSnap.Metadata.Index)
	if err != nil {
		t.Fatal(err)
	}

	g, data, err := ss.LoadSnapshot()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	defer data.Close()
	snapData, _ := ioutil.ReadAll(data)
	if !reflect.DeepEqual(g, testSnap) {
		t.Errorf("snap = %#v, want %#v", g, testSnap)
	}
	if !bytes.Equal(snapData, testSnap.Data) {
		t.Errorf("snap = %#v, want %#v", snapData, testSnap.Data)
	}
}

func TestFailback(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "snapshot")
	err := os.Mkdir(dir, 0700)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	large := fmt.Sprintf("%016x-%016x-%016x.snap", 0xFFFF, 0xFFFF, 0xFFFF)
	err = ioutil.WriteFile(filepath.Join(dir, large), []byte("bad data"), 0666)
	if err != nil {
		t.Fatal(err)
	}

	ss := New(dir)
	err = ss.SaveSnapshot(*testSnap, bytes.NewReader(testSnap.Data))
	if err != nil {
		t.Fatal(err)
	}

	g, data, err := ss.LoadSnapshot()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	defer data.Close()
	g.Data, _ = ioutil.ReadAll(data)
	if !reflect.DeepEqual(g, testSnap) {
		t.Errorf("snap = %#v, want %#v", g, testSnap)
	}
	if f, err := os.Open(filepath.Join(dir, large) + ".broken"); err != nil {
		t.Fatal("broken snapshot does not exist")
	} else {
		f.Close()
	}
}

func TestSnapNames(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "snapshot")
	err := os.Mkdir(dir, 0700)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	for i := 1; i <= 5; i++ {
		var f *os.File
		if f, err = os.Create(filepath.Join(dir, fmt.Sprintf("%d.snap", i))); err != nil {
			t.Fatal(err)
		} else {
			f.Close()
		}
	}
	ss := New(dir)
	names, err := ss.snapNames()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if len(names) != 5 {
		t.Errorf("len = %d, want 10", len(names))
	}
	w := []string{"5.snap", "4.snap", "3.snap", "2.snap", "1.snap"}
	if !reflect.DeepEqual(names, w) {
		t.Errorf("names = %v, want %v", names, w)
	}
}

func TestLoadNewestSnap(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "snapshot")
	err := os.Mkdir(dir, 0700)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	ss := New(dir)
	err = ss.SaveSnapshot(*testSnap, bytes.NewReader(testSnap.Data))
	if err != nil {
		t.Fatal(err)
	}

	newSnap := *testSnap
	newSnap.Metadata.Index = 5
	err = ss.SaveSnapshot(newSnap, bytes.NewReader(newSnap.Data))
	if err != nil {
		t.Fatal(err)
	}

	g, data, err := ss.LoadSnapshot()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	defer data.Close()
	g.Data, _ = ioutil.ReadAll(data)
	if !reflect.DeepEqual(g, &newSnap) {
		t.Errorf("snap = %#v, want %#v", g, &newSnap)
	}
}

func TestNoSnapshot(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "snapshot")
	err := os.Mkdir(dir, 0700)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	ss := New(dir)
	_, _, err = ss.LoadSnapshot()
	if err != ErrNoSnapshot {
		t.Errorf("err = %v, want %v", err, ErrNoSnapshot)
	}
}

func TestEmptySnapshot(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "snapshot")
	err := os.Mkdir(dir, 0700)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	err = ioutil.WriteFile(filepath.Join(dir, "1.snap"), []byte(""), 0x700)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = readSnap(filepath.Join(dir, "1.snap"))
	if err != ErrEmptySnapshot {
		t.Errorf("err = %v, want %v", err, ErrEmptySnapshot)
	}
}

// TestAllSnapshotBroken ensures snapshotter returns
// ErrNoSnapshot if all the snapshots are broken.
func TestAllSnapshotBroken(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "snapshot")
	err := os.Mkdir(dir, 0700)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	err = ioutil.WriteFile(filepath.Join(dir, "1.snap"), []byte("bad"), 0x700)
	if err != nil {
		t.Fatal(err)
	}

	ss := New(dir)
	_, _, err = ss.LoadSnapshot()
	if err != ErrNoSnapshot {
		t.Errorf("err = %v, want %v", err, ErrNoSnapshot)
	}
}

func createSnapBody(t *testing.T, merged *internal_snap.Message) io.ReadCloser {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, uint64(merged.Message.Size())); err != nil {
		t.Fatalf("unable to encode. Error %v", err)
		return nil
	}
	if _, err := buf.Write(pbutil.MustMarshal(&merged.Message)); err != nil {
		t.Fatalf("unable to encode. Error %v", err)
		return nil
	}

	return &pioutil.ReaderAndCloser{
		Reader: io.MultiReader(buf, merged.ReadCloser),
		Closer: merged.ReadCloser,
	}
}
