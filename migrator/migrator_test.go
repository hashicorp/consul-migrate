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
	if _, err := NewMigrator("/leprechauns"); err == nil {
		t.Fatalf("should fail")
	}

	// Create a test Raft directory
	dir := testRaftDir(t)
	defer os.RemoveAll(dir)

	// Initializes the stores correctly
	m, err := NewMigrator(dir)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if m.mdbStore == nil {
		t.Fatalf("missing mdb store")
	}
	if m.boltStore == nil {
		t.Fatalf("missing bolt store")
	}
}

func TestMigrator_migrate(t *testing.T) {
	dir := testRaftDir(t)
	defer os.RemoveAll(dir)

	// Create the migrator
	m, err := NewMigrator(dir)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer m.Close()

	// Perform the migration
	if _, err := m.Migrate(); err != nil {
		t.Fatalf("err: %s %s", err)
	}

	// Check that the new BoltStore was created
	if _, err := os.Stat(filepath.Join(dir, raftPath, boltFilename)); err != nil {
		t.Fatalf("missing bolt file: %s", err)
	}

	// Check that the MDB store was backed up
	if _, err := os.Stat(filepath.Join(dir, raftPath, mdbPath)); err == nil {
		t.Fatalf("MDB dir was not moved")
	}
	if _, err := os.Stat(filepath.Join(dir, raftPath, mdbBackupPath)); err != nil {
		t.Fatalf("Missing MDB backup dir")
	}

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
