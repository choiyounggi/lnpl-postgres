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
