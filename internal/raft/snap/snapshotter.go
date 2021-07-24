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
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	snapSuffix   = ".snap"
	snapDBSuffix = ".db"
)

var (
	ErrNoSnapshot      = errors.New("snap: no available snapshot")
	ErrEmptySnapshot   = errors.New("snap: empty snapshot")
	ErrInvalidSnapshot = errors.New("snap: invalid snapshot")
)

type Snapshotter struct {
	dir string
}

func New(dir string) *Snapshotter {
	return &Snapshotter{
		dir: dir,
	}
}

func (s *Snapshotter) SaveSnapshot(snapshot raftpb.Snapshot, stream io.Reader) error {
	if raft.IsEmptySnap(snapshot) {
		return nil
	}
	return s.saveSnapshot(&snapshot, stream)
}

func (s *Snapshotter) saveSnapshot(snapshot *raftpb.Snapshot, stream io.Reader) error {
	snapFile := s.snapFileName(snapshot)
	err := s.writeSnap(snapFile, snapshot, stream, 0666)
	if err != nil {
		err1 := os.Remove(snapFile)
		if err1 != nil {
			log.Printf("ERROR - failed to remove broken snapshot file %s", snapFile)
		}
	}
	return err
}

func (s *Snapshotter) snapFileName(snapshot *raftpb.Snapshot) string {
	fname := fmt.Sprintf("%016x-%016x%s", snapshot.Metadata.Term, snapshot.Metadata.Index, snapSuffix)
	snapFile := filepath.Join(s.dir, fname)
	return snapFile
}

func (s *Snapshotter) writeSnap(snapFile string, snapshot *raftpb.Snapshot, data io.Reader, perm os.FileMode) error {
	f, err := os.OpenFile(snapFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
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

	if data != nil {
		_, err = io.Copy(f, data)
		if err != nil {
			return err
		}
	}

	err = fileutil.Fsync(f)
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

func (s *Snapshotter) LoadSnapshot() (*raftpb.Snapshot, io.ReadCloser, error) {
	names, err := s.snapNames()
	if err != nil {
		return nil, nil, err
	}
	var (
		snap *raftpb.Snapshot
		data io.ReadCloser
	)
	for _, name := range names {
		if strings.HasSuffix(name, snapSuffix) {
			if snap, data, err = loadSnap(s.dir, name); err == nil {
				break
			}
		}
	}
	if err != nil {
		return nil, nil, ErrNoSnapshot
	}
	return snap, data, nil
}

func (s *Snapshotter) LoadDBSnapshot() (io.ReadCloser, error) {
	names, err := s.snapNames()
	if err != nil {
		return nil, err
	}
	var	data io.ReadCloser
	for _, name := range names {
		if strings.HasSuffix(name, snapDBSuffix) {
			fpath := filepath.Join(s.dir, name)
			if data, err = readSnapDB(fpath); err == nil {
				break
			}
		}
	}
	if err != nil {
		return nil, ErrNoSnapshot
	}
	return data, nil
}

func (s *Snapshotter) LoadSnapshotBody(snapshot raftpb.Snapshot) (io.ReadCloser, error) {
	snapFileName := s.snapFileName(&snapshot)
	_, data, err := readSnap(snapFileName)
	if err != nil {
		log.Printf("ERROR - cannot access snapshot file %v: %v", snapFileName, err)
		return nil, err
	}
	return data, nil
}

func loadSnap(dir, name string) (snap *raftpb.Snapshot, data io.ReadCloser, err error) {
	fpath := filepath.Join(dir, name)
	snap, data, err = readSnap(fpath)
	if err != nil {
		renameBroken(fpath)
	}
	return
}

func readSnapDB(snapName string) (io.ReadCloser, error) {
	snapFile, err := os.Open(snapName)
	if err != nil {
		log.Printf("ERROR - cannot read snapshot DB file %v: %v", snapName, err)
		return nil, err
	}

	return snapFile, nil
}

func readSnap(snapName string) (*raftpb.Snapshot, io.ReadCloser, error) {
	snapFile, err := os.Open(snapName)
	if err != nil {
		log.Printf("ERROR - cannot read file %v: %v", snapName, err)
		return nil, nil, err
	}

	snapLenBts := make([]byte, 4)
	numRead, err := snapFile.Read(snapLenBts)
	if numRead != len(snapLenBts) || err != nil {
		log.Printf("ERROR - unable to read file %v: %v", snapName, err)
		return nil, nil, ErrEmptySnapshot
	}

	snapLen := binary.LittleEndian.Uint32(snapLenBts)
	snapBts := make([]byte, snapLen)
	numRead, err = snapFile.Read(snapBts)
	if err != nil {
		log.Printf("ERROR - unable to read snapshot data from file %v: %v", snapName, err)
		return nil, nil, err
	}
	if numRead != len(snapBts) {
		log.Printf("ERROR - unable to read snapshot data fully from file %v. Expected snapshot length: %d, actual: %d",
			snapName, snapLen, numRead)
		return nil, nil, ErrInvalidSnapshot
	}

	snap := new(raftpb.Snapshot)
	pbutil.MustUnmarshal(snap, snapBts)
	return snap, snapFile, nil
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
		if strings.HasSuffix(names[i], snapSuffix) || strings.HasSuffix(names[i], snapDBSuffix) {
			snaps = append(snaps, names[i])
		} else {
			log.Printf("WARNING - skipped unexpected non snapshot file %v", names[i])
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
