package main

import (
	"fmt"
	"os"

	"github.com/hashicorp/raftutil/migrator"
)

func main() {
	os.Exit(realMain(os.Args))
}

func realMain(args []string) int {
	if len(args) != 2 {
		fmt.Println(usage())
		return 1
	}

	m, err := migrator.NewMigrator(args[1])
	if err != nil {
		fmt.Println(err.Error())
		return 1
	}

	if err := m.Migrate(); err != nil {
		fmt.Println(err.Error())
		return 1
	}
	return 0
}

func usage() string {
	return "Usage: raftutil <consul datadir>"
}
