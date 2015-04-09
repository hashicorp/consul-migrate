package main

import (
	"fmt"
	"os"

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

	m, err := migrator.NewMigrator(args[1])
	if err != nil {
		fmt.Println(err.Error())
		return 1
	}
	defer m.Close()

	if err := m.Migrate(); err != nil {
		fmt.Println(err.Error())
		return 1
	}
	return 0
}

func usage() string {
	return `Usage: consul-migrate <data-dir>

Consul-migrate is a tool for moving Consul server data from LMDB to BoltDB.
This is a prerequisite for upgrading to Consul >= 0.5.1.

This utility migrates both the Raft log and the KV store, and preserves all
data and indexes. The original MDB data folder will *NOT* be modified during
this process, nor will it be automatically deleted. If anything should fail,
the migration can be re-attempted.
`
}
