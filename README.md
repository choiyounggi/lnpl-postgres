# lnpl-postgres

PostgreSQL binding for [lnpl](https://github.com/choiyounggi/linkly)'s
`RepositoryDriver` SPI. Implements [linkly#121](https://github.com/choiyounggi/linkly/issues/121):
a real-Postgres `RepositoryDriver`, verified against
`lnpl.testing.RepositoryDriverTCK` in CI via Testcontainers, and registered
under the `lnpl.drivers` entry-point group as `postgres`.

## Install

```
pip install lnpl-postgres
```

This also installs a matching `lnpl` from PyPI. To try unreleased changes,
install from `main` instead:
`pip install git+https://github.com/choiyounggi/lnpl-postgres@main`.

## Usage

Once installed, `lnpl` discovers this package's driver automatically through
the `lnpl.drivers` entry-point. `--backend` splits on the first colon into
`<scheme>:<arg>`; for `postgres`, `<arg>` is passed verbatim to `make_driver`
as a psycopg conninfo string or URI, e.g.:

```
--backend postgres:postgresql://user:pass@host:5432/dbname
```

A keyword/value conninfo string also works, since `<arg>` is passed through
unparsed:

```
--backend "postgres:host=localhost dbname=app"
```

## Connection pool

`lnpl serve` builds one driver per request. Each driver borrows a connection
from a pool shared by every driver with the same DSN in the process, and
returns it when the driver is closed. So the number of server connections
one process holds never goes above the pool's maximum, however many requests
arrive at once (issue #4). A connection returned in the middle of an
uncommitted transaction is rolled back before it is lent again.

| Environment variable | Default | Meaning |
|---|---|---|
| `LNPL_PG_POOL_MIN_SIZE` | `2` | connections kept open while idle (`0` allowed) |
| `LNPL_PG_POOL_MAX_SIZE` | `20` | most connections this process opens to one DSN |
| `LNPL_PG_POOL_TIMEOUT` | `10` | seconds a driver waits for a free connection before failing with `DriverError` |

The values are read once, when the first driver for a DSN is created, and
must be whole numbers (`MIN_SIZE` at most `MAX_SIZE`; `MAX_SIZE` and
`TIMEOUT` at least 1), otherwise `ValueError`. With several server processes
(for example gunicorn workers), each has its own pool, so keep
`processes × LNPL_PG_POOL_MAX_SIZE` below the server's `max_connections`
(100 in the stock image). The pool is built lazily in each process, so it
works with gunicorn's default fork model; do not use `--preload`.

## Idempotency-Key

`lnpl serve` honours the `Idempotency-Key` request header on this backend:
the driver implements `idempotency_begin`, `idempotency_finish` and
`idempotency_release`. Claims live in the table `lnpl_idempotency`, keyed by
`(workflow_id, key)` and created by the first driver for a DSN with
`CREATE TABLE IF NOT EXISTS`, so there is no separate migration step.

A claim is committed the moment it is made. The recorded status and body are
committed on their own, outside the workflow's transaction, so a run that
fails and rolls back still leaves its result for the next request with the
same key to replay.

A claim older than `lnpl serve`'s idempotency TTL (`LNPL_IDEMPOTENCY_TTL_S`,
default 24 hours) is deleted the next time the same key is claimed. Rows of
keys that are never sent again stay in the table; `created_at` is epoch
milliseconds, so they can be pruned with, for example,
`DELETE FROM lnpl_idempotency WHERE created_at < <epoch milliseconds>`.

If one of these methods is called while the same driver still has a
workflow transaction open, it borrows a second connection from the pool, so
its write is not undone when that transaction rolls back. In `lnpl serve`
this happens only when an event delivery's workflow raised an unexpected
error that left its transaction open and the server then releases that
delivery's claim. If the pool has no free connection at that moment (always
the case with `LNPL_PG_POOL_MAX_SIZE=1`, and with any size once every
connection is in use), that release fails with `DriverError` after
`LNPL_PG_POOL_TIMEOUT` seconds and the key stays in progress until the
idempotency TTL clears it.

## Outbox drain and relay

`lnpl outbox drain`, `lnpl outbox ack` and
`lnpl relay <source> --backend postgres:<dsn> --target <url>` work on this
backend: the driver implements `drain_outbox` and `ack_outbox`, so
`supports_outbox_drain()` is True. A drain lists the rows of `lnpl_outbox`
whose `delivered_at` is empty, lowest `seq` first. An ack sets `delivered_at`
(epoch milliseconds) and never deletes the row. Acking a seq that is already
delivered does nothing and succeeds; a batch that names a seq with no row is
refused as a whole and marks nothing.

A drain locks the rows it returns with `FOR UPDATE SKIP LOCKED`. The `lnpl`
commands call it outside any transaction, so that lock ends as soon as the
drain returns: two `lnpl relay` processes on one database can both send the
same row. That is the at-least-once contract; each event's CloudEvents `id` is
`outbox-<seq>`, so a receiver can drop repeats. Code that calls `begin()`
before `drain_outbox()` keeps the drained rows locked until its `commit()` or
`rollback()`: other drainers skip those rows, and an ack of them from another
connection waits until that transaction ends.

## Local testing

Requires Docker (used by Testcontainers to spin up a real Postgres instance).

```
pip install -e .[test]
python -m unittest discover -s tests
```

On macOS with Docker Desktop, Testcontainers' Ryuk reaper container can fail
to start with a socket-mount error (`mount source path
'.../.docker/run/docker.sock'`). If you hit that, disable Ryuk for the run
(CI's `ubuntu-latest` runners don't need this):

```
TESTCONTAINERS_RYUK_DISABLED=true python -m unittest discover -s tests
```

## Supported lnpl versions

This package depends on `lnpl>=0.9,<0.10` from PyPI, so any lnpl 0.9.x
release works. 0.9.0 is the first lnpl release with
`RepositoryDriver.supports_idempotency()` and the `RepositoryDriverTCK` cases
that check the idempotency methods, so an older lnpl cannot verify them.

The `linkly-sync` workflow checks two things on every PR:

- `latest-release` fails when PyPI has a newer lnpl that this range excludes
  (for example 0.10.0). Then widen the upper bound, run the suite, and release.
- `latest-main` runs the suite against linkly's `main`, so a break shows up
  before linkly releases it.

## Releasing

1. Set `version` in `pyproject.toml` and merge that to `main`.
2. Tag the merge commit `v<version>` and push the tag.
3. `release.yml` runs the suite, builds the sdist and wheel, and publishes them
   to PyPI through Trusted Publishing. The PyPI publisher is: project
   `lnpl-postgres`, owner `choiyounggi`, repository `lnpl-postgres`, workflow
   `release.yml`, environment `pypi`.

PyPI never accepts the same version twice. If the publish job fails for an
outside reason (PyPI outage, publisher not registered), re-run only that job
with `gh run rerun <run-id> --failed`. If the cause is in this repo, fix it on
`main` and release the next patch version. Never delete or re-push a tag.
