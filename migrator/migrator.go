package migrator

import (
	"path/filepath"
	"runtime"

	"github.com/armon/gomdb"
	"github.com/boltdb/bolt"
)

const (
	dbLogs   = "logs"
	dbConf   = "conf"
	raftPath = "raft"

	mdbPath = "mdb"
	mdbMode = 0755

	boltFilename = "raft.db"
	boltFileMode = 0600

	// Maximum log sizes for LMDB. These mirror settings in Consul
	// and are automatically set based on the runtime.
	maxLogSize32bit uint64 = 8 * 1024 * 1024 * 1024
	maxLogSize64bit uint64 = 64 * 1024 * 1024 * 1024
)

type Migrator struct {
	dataDir string
	mdb     *mdb.Env
	bolt    *bolt.DB
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

	// Calculate and set the max size
	size := maxLogSize32bit
	if runtime.GOARCH == "amd64" {
		size = maxLogSize64bit
	}
	if err := env.SetMapSize(size); err != nil {
		return err
	}

	// Open the connection
	err = env.Open(filepath.Join(m.dataDir, mdbPath), mdb.NOTLS, mdbMode)
	if err != nil {
		return err
	}

	// Return the new environment
	m.mdb = env
	return nil
}

func (m *Migrator) boltConnect() error {
	// Connect to the new bolt raft store
	file := filepath.Join(m.dataDir, raftPath, boltFilename)
	b, err := bolt.Open(file, boltFileMode, nil)
	if err != nil {
		return err
	}
	m.bolt = b
	return nil
}

func (m *Migrator) migrateStableStore() error {
	// Begin a new BoltDB transaction
	btx, err := m.bolt.Begin(true)
	if err != nil {
		return err
	}
	defer btx.Rollback()
	bucket := btx.Bucket([]byte(dbConf))

	// Begin a transaction
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
		if err := bucket.Put(k, v); err != nil {
			return err
		}
		if err := btx.Commit(); err != nil {
			return err
		}
		if _, _, err := mcurs.Get(nil, mdb.NEXT); err != nil {
			return nil
		}
	}
}

func (m *Migrator) Migrate() error {
	return m.migrateStableStore()
}
