consul-migrate
==============

consul-migrate is a Go package and CLI utility to perform a very specific
data migration for Consul servers nodes. Between Consul versions 0.5.0
and 0.5.1, the backend for storing Raft data was changed from LMDB to
BoltDB. To support seamless upgrades, this library is embedded in Consul
version 0.5.1 to perform the upgrade automatically.

Supported Upgrade Paths
=======================

Following is a table detailing the supported upgrade paths for Consul.

| From Version | To Version | Procedure                         |
|--------------|------------|-----------------------------------|
| < 0.5.1      | 0.5.1      | Start Consul v0.5.1 normally.     |
| < 0.5.1      | 0.6.0+     | Run `consul-migrate` CLI utility. |

CLI Usage
=========

The consul-migrate CLI is very easy to use. It takes only a single argument:
the path to the consul data-dir. Everything else is handled automatically.

```
Usage: consul-migrate <data-dir>
```

What happens to my data?
========================

The following is the high-level overview of what happens when
consul-migrate is invoked, either as a function or using the CLI:

1. The provided data-dir is checked for and `mdb` sub-dir. If it exists,
   the migration procedure continues to 2. If it does not exist, the
   data-dir is checked for the `raft/raft.db` file. If it exists, this
   indicates the migration has already completed, and the program exits
   with success. If it does not exist, the data-dir is not a consul
   data-dir, and the program exits with an error.

2. A new BoltDB file is created in the data-dir, at `raft/raft.db.temp`.
   All of the Consul data will be migrated to this new DB file.

3. Both LMDB and the BoltDB stores are opened. Data is copied out of LMDB
   and into BoltDB. No write operations are performed on LMDB.

4. The `raft/raft.db.temp` file is moved to `raft/raft.db`. This is the
   location where Consul expects to find the bolt file.

5. The `mdb` directory in the data-dir is renamed to `mdb.backup`. This
   prevents the migration from re-running. At this point, the data is
   successfully migrated and ready to use.

If any of the above steps encounter errors, the entire process is aborted,
and the temporary BoltDB file is removed. The migration can be retried
without negative consequences.
