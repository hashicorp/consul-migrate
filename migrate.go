package raftutil

import (
	"github.com/hashicorp/raft"
)

type Migrator interface {
	// ListStableStore lists all of the keys available in the StableStore.
	// This is used to generate a slice to iterate over during migration.
	ListStableStore() ([][]byte, error)

	// GetStableStore is used to retrieve the value of a single key in
	// the StableStore. This value will be used to create a new entry in
	// the new StableStore.
	GetStableStore(key []byte) ([]byte, error)

	// SetStableStore writes a value into the new StableStore. The value
	// should be identical to that of GetStableStore upon completion.
	SetStableStore(key, val []byte) error

	// ListLogStore lists each key contained in a LogStore. This is used
	// to generate a list to iterate over during a migration.
	ListLogStore() ([][]byte, error)

	// GetLogStore retrieves the value of a single key in the LogStore.
	// This value will be used to write a new log into the new LogStore.
	GetLogStore(key []byte) ([]byte, error)

	// SetLogStore is used to write a log entry into the new LogStore.
	// Upon completion this value should mirror that of GetLogStore.
	SetLogStore(key, val []byte) error
}
