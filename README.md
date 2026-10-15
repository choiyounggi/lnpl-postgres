# lnpl-postgres

PostgreSQL binding for [lnpl](https://github.com/choiyounggi/linkly)'s
`RepositoryDriver` SPI. Implements [linkly#121](https://github.com/choiyounggi/linkly/issues/121):
a real-Postgres `RepositoryDriver`, verified against
`lnpl.testing.RepositoryDriverTCK` in CI via Testcontainers, and registered
under the `lnpl.drivers` entry-point group as `postgres`.

## Install

This package is GitHub-only (not published to PyPI) because it pins `lnpl`
via a PEP 508 direct reference to a specific commit SHA, which PyPI rejects.

```
pip install git+https://github.com/choiyounggi/lnpl-postgres@main
```

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

## Bumping the pinned lnpl commit

This package depends on `lnpl` via a commit-SHA-pinned direct reference in
`pyproject.toml` (`lnpl @ git+https://github.com/choiyounggi/linkly@<sha>`)
rather than `@main`, since linkly's `main` branch moves frequently across
parallel sessions and an unpinned dependency could break CI without warning.
To pick up a newer `lnpl`, replace `<sha>` in the `dependencies` entry with
the target commit SHA from the `linkly` repository and re-run the test suite.

The current pin is `b1cd52354df6d935286f8dc089b9ee1bcb87e673` (lnpl 0.8.0,
linkly PR #256). It is the first linkly `main` commit with
`RepositoryDriver.supports_idempotency()` and the `RepositoryDriverTCK` cases
that check the idempotency methods, so an older pin cannot verify them.
