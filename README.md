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
