package main

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestMain_fails(t *testing.T) {
	// Returns 1 on bad args
	if code := realMain([]string{}); code != 1 {
		t.Fatalf("bad: %d", code)
	}
	if code := realMain([]string{"1", "2", "3"}); code != 1 {
		t.Fatalf("bad: %d", code)
	}

	// Returns 1 on bad data-dir
	if code := realMain([]string{"consul-migrate", "/unicorns"}); code != 1 {
		t.Fatalf("bad: %d", code)
	}
}

func TestMain_help(t *testing.T) {
	fh, err := ioutil.TempFile("", "consul-migrate")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer os.Remove(fh.Name())

	stdoutOrig := *os.Stdout
	os.Stdout = fh
	defer func() {
		os.Stdout = &stdoutOrig
	}()

	if code := realMain([]string{"consul-migrate", "-h"}); code != 0 {
		t.Fatalf("bad: %d", code)
	}

	if _, err := fh.Seek(0, 0); err != nil {
		t.Fatalf("err: %s", err)
	}
	out, err := ioutil.ReadAll(fh)
	if !strings.HasPrefix(string(out), "Usage:") {
		t.Fatalf("bad: %s", string(out))
	}
}

func TestMain_noop(t *testing.T) {
	fh, err := ioutil.TempFile("", "consul-migrate")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer os.Remove(fh.Name())

	stdoutOrig := *os.Stdout
	os.Stdout = fh
	defer func() {
		os.Stdout = &stdoutOrig
	}()

	dir, err := ioutil.TempDir("", "consul-migrate")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer os.RemoveAll(dir)

	if code := realMain([]string{"consul-migrate", dir}); code != 0 {
		t.Fatalf("bad: %d", code)
	}

	if _, err := fh.Seek(0, 0); err != nil {
		t.Fatalf("err: %s", err)
	}
	out, err := ioutil.ReadAll(fh)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !strings.HasPrefix(string(out), "Nothing to do") {
		t.Fatalf("bad: %s", string(out))
	}
}
