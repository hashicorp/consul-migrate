package migrator

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/raft-mdb"
)

const (
	// Path to the raft directory
	raftPath = "raft"

	// Path to the legacy MDB data and its backup location
	mdbPath       = "mdb"
	mdbBackupPath = "mdb.backup"

	// The name of the BoltDB file in the raft path
	boltFilename = "raft.db"

	// Maximum log sizes for LMDB. These mirror settings in Consul
	// and are automatically set based on the runtime.
	maxLogSize32bit uint64 = 8 * 1024 * 1024 * 1024
	maxLogSize64bit uint64 = 64 * 1024 * 1024 * 1024
)

var (
	// Common error messages from migrator
	errFirstIndexZero = fmt.Errorf("No logs found (first index was 0)")
	errLastIndexZero  = fmt.Errorf("No logs found (last index was 0)")

	// stableStoreKeys are the well-known keys written to the
	// stable store, and are used internally by Raft. We hard-code
	// them here so that we can copy them explicitly.
	stableStoreKeys [][]byte = [][]byte{
		[]byte("CurrentTerm"),
		[]byte("LastVoteTerm"),
		[]byte("LastVoteCand"),
	}
)

// Migrator is used to migrate the Consul data storage format on
// servers with versions <= 0.5.0. Consul versions >= 0.5.1 use
// BoltDB internally as the store for the Raft log. During this
// transition, it is necessary to copy data out of our LMDB store
// and create a new BoltStore with the same data.
type Migrator struct {
	dataDir   string                // The Consul data-dir
	mdbStore  *raftmdb.MDBStore     // The legacy MDB environment
	boltStore *raftboltdb.BoltStore // Handle for the new store
}

// New creates a new Migrator given the path to a Consul
// data-dir. Returns the new Migrator and any error.
func New(dataDir string) (*Migrator, error) {
	// Check that the directory exists
	if _, err := os.Stat(dataDir); err != nil {
		return nil, err
	}

	// Create the struct
	m := &Migrator{
		dataDir: dataDir,
	}

	return m, nil
}

// mdbConnect is used to open a handle on our LMDB database. It is
// necessary to use a raw MDB connection here because the Raft
// interface alone does not lend itself to this migration task.
func (m *Migrator) mdbConnect() error {
	// Calculate and set the max size
	size := maxLogSize32bit
	if runtime.GOARCH == "amd64" {
		size = maxLogSize64bit
	}

	// Open the connection
	dir := filepath.Join(m.dataDir, raftPath)
	mdb, err := raftmdb.NewMDBStoreWithSize(dir, size)
	if err != nil {
		return err
	}

	// Return the new environment
	m.mdbStore = mdb
	return nil
}

// boltConnect creates a new BoltStore to copy our data into. We can
// use the BoltStore directly because it provides simple setter
// methods, provided our keys and values are known.
func (m *Migrator) boltConnect() error {
	// Connect to the new BoltStore
	file := filepath.Join(m.dataDir, raftPath, boltFilename)
	store, err := raftboltdb.NewBoltStore(file)
	if err != nil {
		return err
	}

	m.boltStore = store
	return nil
}

// migrateStableStore copies values out of the origin StableStore
// and writes them into the destination. There are only a handful
// of keys we need, so we copy them explicitly.
func (m *Migrator) migrateStableStore() error {
	for _, key := range stableStoreKeys {
		val, err := m.mdbStore.Get(key)
		if err != nil {
			if err.Error() != "not found" {
				return fmt.Errorf("Error getting key '%s': %s", string(key), err)
			}
			continue
		}
		if err := m.boltStore.Set(key, val); err != nil {
			return fmt.Errorf("Error storing key '%s': %s", string(key), err)
		}
	}
	return nil
}

// migrateLogStore is like migrateStableStore, but iterates over
// all of our Raft logs and copies them into the new BoltStore.
func (m *Migrator) migrateLogStore() error {
	first, err := m.mdbStore.FirstIndex()
	if err != nil {
		return err
	}
	if first == 0 {
		return errFirstIndexZero
	}

	last, err := m.mdbStore.LastIndex()
	if err != nil {
		return err
	}
	if last == 0 {
		return errLastIndexZero
	}

	for i := first; i <= last; i++ {
		log := &raft.Log{}
		if err := m.mdbStore.GetLog(i, log); err != nil {
			return err
		}
		if err := m.boltStore.StoreLog(log); err != nil {
			return err
		}
	}
	return nil
}

// archiveMDB moves the original MDB data directory to an archived
// version. This allows the migrator to return fast if the data
// has already been migrated to BoltDB.
func (m *Migrator) archiveMDB() error {
	orig := filepath.Join(m.dataDir, raftPath, mdbPath)
	backup := filepath.Join(m.dataDir, raftPath, mdbBackupPath)
	return os.Rename(orig, backup)
}

// Migrate is the high-level function we call when we want to attempt
// to migrate all of our LMDB data into BoltDB. If an error is
// encountered, the BoltStore is nuked from disk, since it is useless.
// The migration can be attempted again, as the LMDB data should
// still be intact. Returns a bool indicating whether a migration
// was completed, and any error.
func (m *Migrator) Migrate() (bool, error) {
	boltFile := filepath.Join(m.dataDir, raftPath, boltFilename)
	mdbDir := filepath.Join(m.dataDir, raftPath, mdbPath)

	// Check if we should attempt a migration
	if _, err := os.Stat(mdbDir); os.IsNotExist(err) {
		return false, nil
	}

	// Connect the stores
	if err := m.mdbConnect(); err != nil {
		return false, fmt.Errorf("Failed to connect MDB: %s", err)
	}
	defer m.mdbStore.Close()

	if err := m.boltConnect(); err != nil {
		return false, fmt.Errorf("Failed to connect BoltDB: %s", err)
	}
	defer m.boltStore.Close()

	// Migrate the stable store
	if err := m.migrateStableStore(); err != nil {
		os.Remove(boltFile)
		return false, fmt.Errorf("Failed to migrate stable store: %v", err)
	}

	// Migrate the log store
	if err := m.migrateLogStore(); err != nil {
		os.Remove(boltFile)
		return false, fmt.Errorf("Failed to migrate log store: %v", err)
	}

	// Move the old MDB dir to its backup location
	if err := m.archiveMDB(); err != nil {
		os.Remove(boltFile)
		return false, fmt.Errorf("Failed to backup MDB: %v", err)
	}
	return true, nil
}
