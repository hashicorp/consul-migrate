package migrator

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"

	"github.com/armon/gomdb"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

const (
	dbLogs   = "logs"
	dbConf   = "conf"
	raftPath = "raft"

	// MDB path and permission settings.
	mdbPath = "mdb"
	mdbMode = 0755

	// The name of the BoltDB file in the raft path
	boltFilename = "raft.db"

	// Maximum log sizes for LMDB. These mirror settings in Consul
	// and are automatically set based on the runtime.
	maxLogSize32bit uint64 = 8 * 1024 * 1024 * 1024
	maxLogSize64bit uint64 = 64 * 1024 * 1024 * 1024
)

// Migrator is used to migrate the Consul data storage format on
// servers with versions <= 0.5.0. Consul versions >= 0.5.1 use
// BoltDB internally as the store for the Raft log. During this
// transition, it is necessary to copy data out of our LMDB store
// and create a new BoltStore with the same data.
type Migrator struct {
	dataDir   string                // The Consul data-dir
	mdb       *mdb.Env              // The legacy MDB environment
	boltStore *raftboltdb.BoltStore // Handle for the new store
}

// NewMigrator creates a new Migrator given the path to a Consul
// data-dir. Returns the new Migrator and any error.
func NewMigrator(dataDir string) (*Migrator, error) {
	// Create the struct
	m := &Migrator{
		dataDir: dataDir,
	}

	// Connect MDB
	if err := m.mdbConnect(); err != nil {
		return nil, err
	}

	// Connect BoltDB
	if err := m.boltConnect(); err != nil {
		return nil, err
	}
	return m, nil
}

// mdbConnect is used to open a handle on our LMDB database. It is
// necessary to use a raw MDB connection here because the Raft
// interface alone does not lend itself to this migration task.
func (m *Migrator) mdbConnect() error {
	// Create the env
	env, err := mdb.NewEnv()
	if err != nil {
		return err
	}

	// Allow 2 sub-dbs
	if err := env.SetMaxDBs(mdb.DBI(2)); err != nil {
		return err
	}

	// Calculate and set the max size
	size := maxLogSize32bit
	if runtime.GOARCH == "amd64" {
		size = maxLogSize64bit
	}
	if err := env.SetMapSize(size); err != nil {
		return err
	}

	// Open the connection
	err = env.Open(filepath.Join(m.dataDir, raftPath, mdbPath), mdb.NOTLS, mdbMode)
	if err != nil {
		return err
	}

	// Return the new environment
	m.mdb = env
	return nil
}

// boltConnect creates a new BoltStore to copy our data into. We can
// use the BoltStore directly because it provides simple setter
// methods, provided our keys and values are known.
func (m *Migrator) boltConnect() error {
	// Connect to the new BoltStore
	raftFile := filepath.Join(m.dataDir, raftPath, boltFilename)
	store, err := raftboltdb.NewBoltStore(raftFile)
	if err != nil {
		return err
	}

	m.boltStore = store
	return nil
}

// migrateStableStore is used to migrate our base key/value store. It
// uses a cursor to seek to the front of the store and iterate over
// everything so that we can easily get all of our known k/v pairs
// and copy them into the new BoltStore.
func (m *Migrator) migrateStableStore() error {
	// Begin a new MDB transaction
	mtx, err := m.mdb.BeginTxn(nil, mdb.RDONLY)
	if err != nil {
		return err
	}

	// Open the sub-db
	dbi, err := mtx.DBIOpen(dbConf, 0)
	if err != nil {
		mtx.Abort()
		return err
	}
	defer mtx.Abort()

	// Get a new cursor and seek to the first key
	mcurs, err := mtx.CursorOpen(dbi)
	if err != nil {
		return err
	}
	if _, _, err := mcurs.Get(nil, mdb.FIRST); err != nil {
		return err
	}

	// Loop through all of the keys, writing them out to the bolt store
	// as we go. We will stop when we reach the end of the StableStore.
	for {
		// Get the current key
		k, v, err := mcurs.Get(nil, mdb.GET_CURRENT)
		if err != nil {
			return err
		}

		// Write the value into the BoltStore
		if err := m.boltStore.Set(k, v); err != nil {
			return err
		}

		// Move the cursor to the next entry
		if k, _, err := mcurs.Get(nil, mdb.NEXT); err != nil {
			if err == mdb.NotFound || len(k) == 0 {
				return nil
			}
			return err
		}
	}
}

// migrateLogStore is like migrateStableStore, but iterates over
// all of our Raft logs and copies them into the new BoltStore.
func (m *Migrator) migrateLogStore() error {
	// Begin a new MDB transaction
	mtx, err := m.mdb.BeginTxn(nil, mdb.RDONLY)
	if err != nil {
		return err
	}

	// Open the sub-db
	dbi, err := mtx.DBIOpen(dbLogs, 0)
	if err != nil {
		mtx.Abort()
		return err
	}
	defer mtx.Abort()

	// Get a new cursor and seek to the first key
	mcurs, err := mtx.CursorOpen(dbi)
	if err != nil {
		return err
	}
	if _, _, err := mcurs.Get(nil, mdb.FIRST); err != nil {
		return err
	}

	// Loop through all of the keys, writing them out to the bolt store
	// as we go. We will stop when we reach the end of the StableStore.
	for {
		// Get the current key
		_, v, err := mcurs.Get(nil, mdb.GET_CURRENT)
		if err != nil {
			return err
		}

		// Decode the log message
		log := &raft.Log{}
		if err := decodeMsgPack(v, log); err != nil {
			return err
		}

		// Write the value into the BoltStore
		if err := m.boltStore.StoreLog(log); err != nil {
			return err
		}

		// Move the cursor to the next entry
		if k, _, err := mcurs.Get(nil, mdb.NEXT); err != nil {
			if err == mdb.NotFound || len(k) == 0 {
				return nil
			}
			return err
		}
	}
}

// Migrate is the high-level function we call when we want to attempt
// to migrate all of our LMDB data into BoltDB. If an error is
// encountered, the BoltStore is nuked from disk, since it is useless.
// The migration can be attempted again, as the LMDB data should
// still be intact.
func (m *Migrator) Migrate() error {
	if err := m.migrateStableStore(); err != nil {
		os.Remove(filepath.Join(m.dataDir, raftPath, boltFilename))
		return err
	}
	if err := m.migrateLogStore(); err != nil {
		os.Remove(filepath.Join(m.dataDir, raftPath, boltFilename))
		return err
	}
	return nil
}

// decodeMsgPack decodes a msgpack byte sequence
func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}
