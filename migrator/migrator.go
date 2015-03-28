package migrator

import (
	"bytes"
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

	mdbPath = "mdb"
	mdbMode = 0755

	boltFilename = "raft.db"

	// Maximum log sizes for LMDB. These mirror settings in Consul
	// and are automatically set based on the runtime.
	maxLogSize32bit uint64 = 8 * 1024 * 1024 * 1024
	maxLogSize64bit uint64 = 64 * 1024 * 1024 * 1024
)

type Migrator struct {
	dataDir   string
	mdb       *mdb.Env
	boltStore *raftboltdb.BoltStore
}

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

func (m *Migrator) Migrate() error {
	if err := m.migrateStableStore(); err != nil {
		return err
	}
	if err := m.migrateLogStore(); err != nil {
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
