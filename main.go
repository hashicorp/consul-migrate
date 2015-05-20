package main

import (
	"fmt"
	"os"
	"time"

	"github.com/hashicorp/consul-migrate/migrator"
)

func main() {
	os.Exit(realMain(os.Args))
}

func realMain(args []string) int {
	if len(args) != 2 {
		fmt.Println(usage())
		return 1
	}

	// Observe the help flags
	if args[1] == "-h" || args[1] == "--help" {
		fmt.Println(usage())
		return 0
	}

	// Create the migrator
	m, err := migrator.New(args[1])
	if err != nil {
		fmt.Printf("Error creating migrator: %s\n", err)
		return 1
	}

	// Handle progress output
	doneCh := make(chan struct{})
	defer close(doneCh)
	go handleProgress(m.ProgressCh, doneCh)

	// Perform the migration
	start := time.Now()
	migrated, err := m.Migrate()
	if err != nil {
		fmt.Printf("Migration failed: %s\n", err)
		return 1
	}

	// Check the result
	if migrated {
		fmt.Printf("Migration completed in %s\n", time.Now().Sub(start))
	} else {
		fmt.Printf("Nothing to do for directory '%s'\n", args[1])
	}
	return 0
}

// handleProgress is used to dump progress information to the console while
// a migration is in flight. This allows the user to monitor a migration.
func handleProgress(ch <-chan *migrator.ProgressUpdate, doneCh <-chan struct{}) {
	var lastOp string
	var lastProgress float64
	lastFlush := time.Now()
	for {
		select {
		case update := <-ch:
			switch {
			case lastOp != update.Op:
				lastProgress = update.Progress
				lastOp = update.Op
				fmt.Println(update.Op)
				fmt.Printf("%.2f%%\n", update.Progress)

			case update.Progress-lastProgress >= 5:
				fallthrough

			case time.Now().Sub(lastFlush) > time.Second:
				fallthrough

			case update.Progress == 100:
				lastFlush = time.Now()
				lastProgress = update.Progress
				fmt.Printf("%.2f%%\n", update.Progress)
			}
		case <-doneCh:
			return
		}
	}
}

func usage() string {
	return `Usage: consul-migrate <data-dir>

Consul-migrate is a tool for moving Consul server data from LMDB to BoltDB.
This is a prerequisite for upgrading to Consul >= 0.5.1.

This utility will migrate all of the data Consul needs to pick up right where
it left off. The original MDB data folder will *NOT* be modified during the
migration process. If any part of the migration fails, it is safe to try again.
This command is also idempotent, and will not re-attempt a migration which has
already been completed.

Upon successful migration, the MDB data directory will be renamed so that it
includes the ".backup" extension. Once you have verified Consul is operational
after the migration, and contains all of the expected data, it is safe to
archive the "mdb.backup" directory and remove it from the Consul server.

Returns 0 on successful migration or no-op, 1 for errors.
`
}
