package snap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	snapSuffix = ".snap"
)

var (
	ErrNoSnapshot    = errors.New("snap: no available snapshot")
	ErrEmptySnapshot = errors.New("snap: empty snapshot")

	// A map of valid files that can be present in the snap folder.
	validFiles = map[string]bool{
		"db": true,
	}
)

type Snapshotter struct {
	dir string
}

func New(dir string) *Snapshotter {
	return &Snapshotter{
		dir: dir,
	}
}

func (s *Snapshotter) SaveSnapStream(snapshot raftpb.Snapshot, stream io.Reader) error {
	if raft.IsEmptySnap(snapshot) {
		return nil
	}
	return s.saveStream(&snapshot, stream)
}

func (s *Snapshotter) SaveSnap(snapshot raftpb.Snapshot) error {
	if raft.IsEmptySnap(snapshot) {
		return nil
	}
	return s.save(&snapshot)
}

func (s *Snapshotter) saveStream(snapshot *raftpb.Snapshot, stream io.Reader) error {
	fname := fmt.Sprintf("%016x-%016x%s", snapshot.Metadata.Term, snapshot.Metadata.Index, snapSuffix)
	snapFile := filepath.Join(s.dir, fname)
	err := writeAndSyncFile(snapFile, snapshot, stream, 0666)
	if err != nil {
		err1 := os.Remove(snapFile)
		if err1 != nil {
			log.Printf("ERROR - failed to remove broken snapshot file %s", snapFile)
		}
	}
	return err
}

func (s *Snapshotter) save(snapshot *raftpb.Snapshot) error {
	return s.saveStream(snapshot, bytes.NewReader(snapshot.Data))
}

func writeAndSyncFile(filename string, snapshot *raftpb.Snapshot, data io.Reader, perm os.FileMode) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	snapBts := pbutil.MustMarshal(snapshot)
	snapLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(snapLen, uint32(len(snapBts)))

	snapBuff := bytes.Buffer{}
	snapBuff.Write(snapLen)
	snapBuff.Write(snapBts)

	_, err = io.Copy(f, &snapBuff)
	if err != nil {
		return err
	}

	_, err = io.Copy(f, data)
	if err != nil {
		return err
	}

	err = fileutil.Fsync(f)
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

func (s *Snapshotter) Load() (*raftpb.Snapshot, error) {
	names, err := s.snapNames()
	if err != nil {
		return nil, err
	}
	var snap *raftpb.Snapshot
	for _, name := range names {
		if snap, err = loadSnap(s.dir, name); err == nil {
			break
		}
	}
	if err != nil {
		return nil, ErrNoSnapshot
	}
	return snap, nil
}

func loadSnap(dir, name string) (*raftpb.Snapshot, error) {
	fpath := filepath.Join(dir, name)
	snap, err := readSnap(fpath)
	if err != nil {
		renameBroken(fpath)
	}
	return snap, err
}

func readSnap(snapname string) (*raftpb.Snapshot, error) {
	data, err := ioutil.ReadFile(snapname)
	if err != nil {
		log.Printf("ERROR - cannot read file %v: %v", snapname, err)
		return nil, err
	}

	if len(data) == 0 {
		log.Printf("ERROR - unexpected empty snapshot")
		return nil, ErrEmptySnapshot
	}

	if len(data) < 4 {
		return nil, ErrEmptySnapshot
	}
	snapLen := binary.LittleEndian.Uint32(data[:4])
	data = data[4:]

	if uint32(len(data)) < snapLen {
		return nil, ErrEmptySnapshot
	}
	snapBts := data[:snapLen]
	snap := new(raftpb.Snapshot)
	pbutil.MustUnmarshal(snap, snapBts)
	snap.Data = data[snapLen:]
	return snap, nil
}

// snapNames returns the filename of the snapshots in logical time order (from newest to oldest).
// If there is no available snapshots, an ErrNoSnapshot will be returned.
func (s *Snapshotter) snapNames() ([]string, error) {
	dir, err := os.Open(s.dir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	if err = s.cleanupSnapdir(names); err != nil {
		return nil, err
	}
	snaps := checkSuffix(names)
	if len(snaps) == 0 {
		return nil, ErrNoSnapshot
	}
	sort.Sort(sort.Reverse(sort.StringSlice(snaps)))
	return snaps, nil
}

func checkSuffix(names []string) []string {
	var snaps []string
	for i := range names {
		if strings.HasSuffix(names[i], snapSuffix) {
			snaps = append(snaps, names[i])
		} else {
			// If we find a file which is not a snapshot then check if it's
			// a valid file. If not throw out a warning.
			if _, ok := validFiles[names[i]]; !ok {
				log.Printf("WARNING - skipped unexpected non snapshot file %v", names[i])
			}
		}
	}
	return snaps
}

func renameBroken(path string) {
	brokenPath := path + ".broken"
	if err := os.Rename(path, brokenPath); err != nil {
		log.Printf("WARNING - cannot rename broken snapshot file %v to %v: %v", path, brokenPath, err)
	}
}

// cleanupSnapdir removes any files that should not be in the snapshot directory:
// - db.tmp prefixed files that can be orphaned by defragmentation
func (s *Snapshotter) cleanupSnapdir(filenames []string) error {
	for _, filename := range filenames {
		if strings.HasPrefix(filename, "db.tmp") {
			log.Printf("INFO - found orphaned defragmentation file; deleting: %s", filename)
			if rmErr := os.Remove(filepath.Join(s.dir, filename)); rmErr != nil && !os.IsNotExist(rmErr) {
				return fmt.Errorf("failed to remove orphaned defragmentation file %s: %v", filename, rmErr)
			}
		}
	}
	return nil
}
