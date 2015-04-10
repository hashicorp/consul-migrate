package migrator

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/hashicorp/raft"
)

func testRaftDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "consul-migrate")
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Make the mdb sub-dir
	if err := os.MkdirAll(filepath.Join(dir, "raft", "mdb"), 0700); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Copy the MDB files
	for _, file := range []string{"data.mdb", "lock.mdb"} {
		src, err := os.Open(filepath.Join("test-fixtures", "raft", "mdb", file))
		if err != nil {
			t.Fatalf("err: %s", err)
		}

		dest, err := os.Create(filepath.Join(dir, "raft", "mdb", file))
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if _, err := io.Copy(dest, src); err != nil {
			t.Fatalf("err: %s", err)
		}
		src.Close()
		dest.Close()
	}

	return dir
}

func TestMigrator_new(t *testing.T) {
	// Fails on bad data-dir
	if _, err := New("/leprechauns"); err == nil {
		t.Fatalf("should fail")
	}

	// Works with an existing directory
	dir := testRaftDir(t)
	defer os.RemoveAll(dir)

	if _, err := New(dir); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func TestMigrator_migrate(t *testing.T) {
	dir := testRaftDir(t)
	defer os.RemoveAll(dir)

	// Create the migrator
	m, err := New(dir)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Perform the migration
	if _, err := m.Migrate(); err != nil {
		t.Fatalf("err: %s %s", err)
	}

	// Check that the new BoltStore was created
	if _, err := os.Stat(filepath.Join(dir, raftPath, boltFilename)); err != nil {
		t.Fatalf("missing bolt file: %s", err)
	}

	// Check that the MDB store was backed up
	mdbPathOrig := filepath.Join(dir, raftPath, mdbPath)
	mdbPathBackup := filepath.Join(dir, raftPath, mdbBackupPath)
	if _, err := os.Stat(mdbPathOrig); err == nil {
		t.Fatalf("MDB dir was not moved")
	}
	if _, err := os.Stat(mdbPathBackup); err != nil {
		t.Fatalf("Missing MDB backup dir")
	}

	// Reconnect the data sources. Requires moving the MDB
	// store back to its original location.
	if err := os.Rename(mdbPathBackup, mdbPathOrig); err != nil {
		t.Fatalf("err: %s", err)
	}

	if err := m.mdbConnect(); err != nil {
		t.Fatalf("err: %s", err)
	}
	defer m.mdbStore.Close()

	if err := m.boltConnect(); err != nil {
		t.Fatalf("err: %s", err)
	}
	defer m.boltStore.Close()

	// Check that the BoltStore now has the indexes
	mFirst, err := m.mdbStore.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	mLast, err := m.mdbStore.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	bFirst, err := m.boltStore.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if bFirst != mFirst {
		t.Fatalf("bad: %d", bFirst)
	}
	bLast, err := m.boltStore.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if bLast != mLast {
		t.Fatalf("bad: %d", bLast)
	}

	// Ensure that the logs were copied properly
	for i := mFirst; i <= mLast; i++ {
		mLog := &raft.Log{}
		if err := m.mdbStore.GetLog(i, mLog); err != nil {
			t.Fatalf("err: %s", err)
		}
		bLog := &raft.Log{}
		if err := m.boltStore.GetLog(i, bLog); err != nil {
			t.Fatalf("err: %s", err)
		}
		if !reflect.DeepEqual(mLog, bLog) {
			t.Fatalf("bad: %v %v", mLog, bLog)
		}
	}

	// Ensure the stable store values were copied.
	for _, key := range stableStoreKeys {
		mVal, err := m.mdbStore.Get(key)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		bVal, err := m.boltStore.Get(key)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if !bytes.Equal(mVal, bVal) {
			t.Fatalf("bad value for key '%s'", key)
		}
	}
}
