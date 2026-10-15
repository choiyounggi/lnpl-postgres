"""PostgreSQL RepositoryDriver — postgres mirror of lnpl's SqliteRepositoryDriver.

STATEMENT TEXT IS CONSTANT (lnpl.drivers module docstring): every SQL string
below is a literal; every value that varies rides in as a bound parameter.
"""

import atexit
import contextlib
import json
import os
import threading
import time

import psycopg
import psycopg.errors
import psycopg_pool

from lnpl.drivers import (
    ACCEPTED_OPS,
    ConflictError,
    DriverError,
    READ_OPS,
    RepositoryDriver,
    WriteConflictError,
)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS lnpl_rows (
    entity_id TEXT NOT NULL,
    row_key   TEXT NOT NULL,
    payload   JSONB NOT NULL,
    _version  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (entity_id, row_key)
)
"""
_CREATE_OUTBOX_TABLE = """
CREATE TABLE IF NOT EXISTS lnpl_outbox (
    seq          BIGSERIAL PRIMARY KEY,
    emission_id  TEXT NOT NULL,
    event        TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   BIGINT NOT NULL,
    delivered_at BIGINT
)
"""

_SELECT_ROW = ("SELECT payload, _version FROM lnpl_rows "
              "WHERE entity_id = %s AND row_key = %s")
_SELECT_ALL_ROWS = ("SELECT payload FROM lnpl_rows WHERE entity_id = %s "
                    "ORDER BY row_key")
_SELECT_SORTED = ("SELECT payload FROM lnpl_rows WHERE entity_id = %s "
                  "ORDER BY payload -> %s, row_key")
_SELECT_PREDICATE_OPS = {
    "==": "=", "!=": "!=", "<": "<", "<=": "<=", ">": ">", ">=": ">=",
}
_SELECT_PREDICATE_BASE = "SELECT payload FROM lnpl_rows WHERE entity_id = %s"
_SELECT_PREDICATE_TERM = " AND payload -> %s {op} %s::jsonb"
_SELECT_PREDICATE_ORDER = " ORDER BY payload -> %s{desc}, row_key"
_SELECT_PREDICATE_ORDER_DEFAULT = " ORDER BY row_key"
_SELECT_PREDICATE_LIMIT = " LIMIT %s"
_INSERT_IF_ABSENT = (
    "INSERT INTO lnpl_rows (entity_id, row_key, payload) "
    "VALUES (%s, %s, %s::jsonb) ON CONFLICT (entity_id, row_key) DO NOTHING")
_INSERT_ROW = ("INSERT INTO lnpl_rows (entity_id, row_key, payload) "
              "VALUES (%s, %s, %s::jsonb)")
_UPDATE_ROW = ("UPDATE lnpl_rows SET payload = %s::jsonb, _version = _version + 1 "
              "WHERE entity_id = %s AND row_key = %s")
_UPDATE_ROW_VERSIONED = (
    "UPDATE lnpl_rows SET payload = %s::jsonb, _version = _version + 1 "
    "WHERE entity_id = %s AND row_key = %s AND _version = %s")
_DELETE_ROW = "DELETE FROM lnpl_rows WHERE entity_id = %s AND row_key = %s"

_INSERT_OUTBOX = (
    "INSERT INTO lnpl_outbox (emission_id, event, payload, created_at) "
    "VALUES (%s, %s, %s::jsonb, %s)")
_SELECT_OUTBOX_SINCE = (
    "SELECT seq, emission_id, event, payload, created_at "
    "FROM lnpl_outbox WHERE event = %s AND seq > %s ORDER BY seq")

# issue #8 (t2): the drain/ack consumer's statements. `delivered_at` is that
# consumer's column alone (`read_outbox` ignores it); a row is marked, never
# deleted. A bound `LIMIT NULL` means no limit, so one statement serves
# `drain_outbox(limit=None)` and `drain_outbox(limit=n)`. `FOR UPDATE SKIP
# LOCKED`: rows another open transaction has drained are skipped, not waited
# on; the lock lasts until this connection's transaction ends -- the
# statement itself outside a workflow transaction (autocommit), the
# workflow's commit()/rollback() inside one.
_SELECT_UNDELIVERED = (
    "SELECT seq, emission_id, event, payload, created_at FROM lnpl_outbox "
    "WHERE delivered_at IS NULL ORDER BY seq LIMIT %s FOR UPDATE SKIP LOCKED")
_SELECT_OUTBOX_SEQS = "SELECT seq FROM lnpl_outbox WHERE seq = ANY(%s)"
# The `delivered_at IS NULL` guard makes a re-ack idempotent without a second
# read (sqlite `_MARK_DELIVERED`): an already-delivered seq matches zero rows
# and keeps the delivered_at its first ack set.
_MARK_DELIVERED = (
    "UPDATE lnpl_outbox SET delivered_at = %s "
    "WHERE seq = ANY(%s) AND delivered_at IS NULL")

# issue #113, r1: `(workflow_id, key)` claims a slot the moment a request
# with that `Idempotency-Key` arrives -- INSERTed and committed immediately,
# so a genuinely concurrent second request with the same key sees it right
# away (`idempotency_begin` below). The final disposition (`http_status`/
# `body`) is written by a SEPARATE statement, AFTER `run_workflow` returns,
# deliberately outside that execution's own commit/rollback boundary: the
# plan first had this row's whole lifecycle living inside that boundary, but
# `run_workflow` calls `self.repo.rollback()` unconditionally on any failure
# (`interp.py`), and a rollback there would undo the finalizing UPDATE and
# revert the row to `in-progress` forever -- worse than not having the
# feature. `status` is `in-progress` or `done`; `done` is what a later
# request replays regardless of whether the run it recorded succeeded or
# failed (Stripe's contract: a same-key retry gets back the SAME result,
# never a second execution).
# Postgres: there is no autonomous transaction, so "outside that boundary"
# means "on a connection that is not inside it". While this driver's own
# SQL transaction is open (`_sql_transaction_open`), the idempotency
# statements run on a second connection borrowed from the same pool
# (`_idempotency_connection`); otherwise on the driver's own connection,
# which is autocommit, so each statement commits itself. `created_at` is
# the caller's epoch-millisecond `now_ms`, never the server's clock.
_CREATE_IDEMPOTENCY_TABLE = """
CREATE TABLE IF NOT EXISTS lnpl_idempotency (
    key         TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    status      TEXT NOT NULL,
    http_status INTEGER,
    body        TEXT,
    created_at  BIGINT NOT NULL,
    PRIMARY KEY (workflow_id, key)
)
"""
# Lazy TTL sweep, one key: the row goes only if it is already expired
# (`created_at <= now_ms - ttl_ms`, i.e. NOT `now_ms - created_at < ttl_ms`),
# so a concurrent caller's fresh claim is never deleted.
_DELETE_EXPIRED_IDEMPOTENCY = (
    "DELETE FROM lnpl_idempotency "
    "WHERE workflow_id = %s AND key = %s AND created_at <= %s")
_INSERT_IDEMPOTENCY_CLAIM = (
    "INSERT INTO lnpl_idempotency (key, workflow_id, status, created_at) "
    "VALUES (%s, %s, 'in-progress', %s) "
    "ON CONFLICT (workflow_id, key) DO NOTHING")
_SELECT_IDEMPOTENCY = (
    "SELECT status, http_status, body FROM lnpl_idempotency "
    "WHERE workflow_id = %s AND key = %s")
_UPDATE_IDEMPOTENCY_DONE = (
    "UPDATE lnpl_idempotency SET status = 'done', http_status = %s, body = %s "
    "WHERE workflow_id = %s AND key = %s")
_DELETE_IDEMPOTENCY = (
    "DELETE FROM lnpl_idempotency WHERE workflow_id = %s AND key = %s")


def _encode(row):
    return json.dumps(row, ensure_ascii=False, sort_keys=True)


class _VersionedRow(dict):
    """A row as `_read` found it, carrying the `_version` postgres observed
    at that read. `observed_version` is a plain instance attribute, read
    only by `persist()` to gate the write against a change since this read
    landed."""

    def __init__(self, data, version):
        super().__init__(data)
        self.observed_version = version


# Issue #4: `lnpl serve` builds one driver per request, so a driver that
# opened its own connection made one server connection per request, and
# under load the connections piled up (`too many clients already`). Each
# driver now borrows from one pool per DSN for its lifetime and returns the
# connection in `close()`, so the server-side count stays at most max_size.
_POOL_DEFAULTS = {"min_size": 2, "max_size": 20, "timeout": 10}
# dsn -> (pid that built the pool, pool). A pool inherited across fork()
# has no worker threads and shares sockets with its parent, so a process
# only ever uses (and closes) the pools it built itself.
_POOLS = {}
_POOLS_LOCK = threading.Lock()


def _pool_setting(name):
    """`LNPL_PG_POOL_<NAME>` from the environment, or the default; refused
    with ValueError unless it is a whole number in range."""
    env = "LNPL_PG_POOL_" + name.upper()
    raw = os.environ.get(env)
    if raw is None or not raw.strip():
        return _POOL_DEFAULTS[name]
    try:
        value = int(raw)
    except ValueError:
        raise ValueError("%s must be a whole number, got %r" % (env, raw)) from None
    floor = 0 if name == "min_size" else 1
    if value < floor:
        raise ValueError("%s must be at least %d, got %d" % (env, floor, value))
    return value


def _pool_for(dsn):
    """The process-wide pool for `dsn`, built on first use. Before building
    it, one plain connect proves the DSN reachable, so a bad DSN fails fast
    with DriverError instead of after the pool's wait timeout."""
    pid = os.getpid()
    with _POOLS_LOCK:
        entry = _POOLS.get(dsn)
    if entry is not None and entry[0] == pid:
        return entry[1]
    min_size = _pool_setting("min_size")
    max_size = _pool_setting("max_size")
    timeout = _pool_setting("timeout")
    if min_size > max_size:
        raise ValueError(
            "LNPL_PG_POOL_MIN_SIZE (%d) must not exceed LNPL_PG_POOL_MAX_SIZE (%d)"
            % (min_size, max_size))
    # Network I/O stays outside the lock, so one slow host never stalls
    # lookups of the pools already built for other DSNs.
    try:
        psycopg.connect(dsn).close()
    except psycopg.Error as exc:
        raise DriverError("cannot open the postgres store at %r: %s"
                          % (dsn, exc)) from exc
    pool = psycopg_pool.ConnectionPool(
        dsn, min_size=min_size, max_size=max_size, timeout=timeout,
        kwargs={"autocommit": True},
        check=psycopg_pool.ConnectionPool.check_connection,
        name="lnpl-postgres", open=True)
    with _POOLS_LOCK:
        entry = _POOLS.get(dsn)
        if entry is None or entry[0] != pid:
            _POOLS[dsn] = (pid, pool)
            return pool
    pool.close()  # another thread of this process built one first
    return entry[1]


@atexit.register
def _close_pools():
    pid = os.getpid()
    with _POOLS_LOCK:
        pools = [pool for owner, pool in _POOLS.values() if owner == pid]
        _POOLS.clear()
    for pool in pools:
        pool.close()


def _after_fork_in_child():
    # The child must not wait on a lock some other parent thread held at
    # fork time; inherited pools are skipped by the pid check above.
    global _POOLS_LOCK
    _POOLS_LOCK = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_in_child)


class PostgresRepositoryDriver(RepositoryDriver):
    """A postgres-backed repository. One connection borrowed from the DSN's
    pool for the driver's lifetime (issue #4), always autocommit for reads; a SQL transaction is opened explicitly (`BEGIN`) only around a
    write, never via `psycopg`'s own `conn.commit()`/`conn.rollback()`/
    `conn.transaction()` (the latter allows nested SAVEPOINTs, which this
    driver's own 2-flag boundary tracking does not want)."""

    supports_predicate = True

    def __init__(self, dsn):
        self.raw_dsn = dsn
        if not str(dsn).strip():
            raise ValueError(
                "--backend postgres: needs a DSN, got an empty one "
                "(e.g. --backend postgres:postgresql://user:pass@host/db)")
        self.dsn = dsn
        self._in_transaction = False
        self._sql_transaction_open = False
        # linkly#182 mirror: the exact _VersionedRow execute()'s `read`
        # branch last handed out per (entity_id, key), so _touch's `update`
        # can advance ITS observed_version too (last read under a key wins).
        self._bound_rows = {}
        self._conn = None
        self._pool = _pool_for(dsn)
        try:
            self._conn = self._pool.getconn()
        except psycopg_pool.PoolTimeout as exc:
            stats = self._pool.get_stats()
            if stats.get("pool_size", 0) >= self._pool.max_size \
                    and stats.get("pool_available", 0) == 0:
                reason = ("all %d pooled connections stayed in use "
                          "(LNPL_PG_POOL_MAX_SIZE)" % self._pool.max_size)
            else:
                reason = "the postgres server could not be reached"
            raise DriverError("no postgres connection within %ss: %s"
                              % (self._pool.timeout, reason)) from exc
        try:
            self._conn.execute(_CREATE_TABLE)
            self._conn.execute(_CREATE_OUTBOX_TABLE)
            self._conn.execute(_CREATE_IDEMPOTENCY_TABLE)
        except psycopg.Error as exc:
            self.close()
            raise DriverError("cannot open the postgres store at %r: %s"
                              % (dsn, exc)) from exc

    # -- contract ------------------------------------------------------------

    def seed(self, rows):
        try:
            self._ensure_sql_transaction()
            for entity_id, table in (rows or {}).items():
                for key, row in table.items():
                    self._conn.execute(_INSERT_IF_ABSENT,
                                       (entity_id, key, _encode(row)))
            self._end_write()
        except psycopg.Error as exc:
            raise DriverError("cannot seed the repository: %s" % exc) from exc

    def begin(self):
        if self._in_transaction:
            raise DriverError(
                "begin() called while a transaction is already open — "
                "nested transactions are not supported; commit() or "
                "rollback() the open one first")
        self._in_transaction = True

    def commit(self):
        try:
            if self._sql_transaction_open:
                self._conn.execute("COMMIT")
        except psycopg.Error as exc:
            raise DriverError("cannot commit transaction: %s" % exc) from exc
        finally:
            self._in_transaction = False
            self._sql_transaction_open = False

    def rollback(self):
        try:
            if self._sql_transaction_open:
                self._conn.execute("ROLLBACK")
        except psycopg.Error as exc:
            raise DriverError("cannot roll back transaction: %s" % exc) from exc
        finally:
            self._in_transaction = False
            self._sql_transaction_open = False

    def _ensure_sql_transaction(self):
        if self._in_transaction and not self._sql_transaction_open:
            try:
                self._conn.execute("BEGIN")
            except psycopg.Error as exc:
                raise DriverError("cannot begin transaction: %s" % exc) from exc
            self._sql_transaction_open = True

    def _end_write(self):
        # autocommit mode: a write outside a workflow boundary already
        # committed itself — no-op kept only for symmetry with the
        # boundary-aware call sites above.
        pass

    def execute(self, entity_id, operation, key):
        if operation in READ_OPS:
            row = self._read(entity_id, key)
            if operation == "read" and row is not None:
                # linkly#182 mirror: register here, never inside `_read`,
                # which `_touch` also calls for its own throwaway read.
                self._bound_rows[(entity_id, key)] = row
            return row
        if operation == "create":
            return self._create(entity_id, key)
        if operation in ("update", "delete"):
            return self._touch(entity_id, operation, key)
        raise DriverError("unsupported repository operation %r (accepted: %s)"
                          % (operation, ", ".join(ACCEPTED_OPS)))

    def query(self, entity_id, predicate=None, order=None, limit=None):
        if predicate is None and order is None and limit is None:
            sql, params = _SELECT_ALL_ROWS, (entity_id,)
        else:
            parts = [_SELECT_PREDICATE_BASE]
            params = [entity_id]
            for field, op, value in predicate or ():
                parts.append(_SELECT_PREDICATE_TERM.format(
                    op=_SELECT_PREDICATE_OPS[op]))
                params.append(field)
                params.append(_encode(value))
            if order is not None:
                field, desc = order
                parts.append(_SELECT_PREDICATE_ORDER.format(
                    desc=" DESC" if desc else ""))
                params.append(field)
            else:
                parts.append(_SELECT_PREDICATE_ORDER_DEFAULT)
            if limit is not None:
                parts.append(_SELECT_PREDICATE_LIMIT)
                params.append(limit)
            sql, params = "".join(parts), tuple(params)
        try:
            found = self._conn.execute(sql, params).fetchall()
        except psycopg.Error as exc:
            raise DriverError("cannot query %s: %s" % (entity_id, exc)) from exc
        return [row[0] for row in found]

    def query_sorted(self, entity_id, field):
        try:
            found = self._conn.execute(
                _SELECT_SORTED, (entity_id, field)).fetchall()
        except psycopg.Error as exc:
            raise DriverError("cannot query %s sorted by %s: %s"
                              % (entity_id, field, exc)) from exc
        return [row[0] for row in found]

    def persist(self, entity_id, key, row):
        version = getattr(row, "observed_version", None)
        try:
            self._ensure_sql_transaction()
            if version is None:
                self._conn.execute(_UPDATE_ROW, (_encode(row), entity_id, key))
                self._end_write()
                return
            cursor = self._conn.execute(
                _UPDATE_ROW_VERSIONED, (_encode(row), entity_id, key, version))
            if cursor.rowcount == 0:
                # issue #79 mirror: only roll back locally outside a
                # workflow transaction. Inside one, the execution boundary's
                # own rollback() is what cleans up once this DriverError
                # becomes a RunError and the run is decided failed.
                if not self._in_transaction:
                    self._conn.execute("ROLLBACK")
                raise WriteConflictError(
                    "write conflict: row changed since read (%s %s)"
                    % (entity_id, key))
            # linkly#174 mirror: the UPDATE above bumped `_version`, so the
            # row this caller holds is now one version behind -- advance it,
            # or a second `set` on the same binding is a phantom conflict.
            row.observed_version = version + 1
            self._end_write()
        except psycopg.Error as exc:
            raise DriverError("cannot persist %s: %s" % (entity_id, exc)) from exc

    def record_emission(self, emission):
        try:
            self._ensure_sql_transaction()
            self._conn.execute(
                _INSERT_OUTBOX,
                (emission["emission_id"], emission["event"],
                 _encode(emission["payload"]), int(time.time() * 1000)))
            self._end_write()
        except psycopg.Error as exc:
            raise DriverError("cannot record emission %s: %s"
                              % (emission["emission_id"], exc)) from exc

    def read_outbox(self, event, after_seq=0):
        try:
            found = self._conn.execute(
                _SELECT_OUTBOX_SINCE, (event, after_seq)).fetchall()
        except psycopg.Error as exc:
            raise DriverError("cannot read outbox for %s: %s"
                              % (event, exc)) from exc
        return [{"seq": row[0], "emission_id": row[1], "event": row[2],
                "payload": row[3], "created_at": row[4]}
               for row in found]

    def drain_outbox(self, limit=None):
        """Every undelivered emission, `seq` ascending, as dicts shaped like
        `read_outbox`'s rows; `[]` when there is none, never `None`. `limit`
        caps the batch; a negative `limit` means no cap (sqlite parity).

        The rows returned are locked `FOR UPDATE SKIP LOCKED` on this
        driver's own connection. Outside a workflow transaction that
        connection is autocommit, so the lock ends with the statement and a
        second drain lists the same rows until they are acked (at-least-once).
        Inside one (`begin()` ... `commit()`/`rollback()`) the drain opens
        the SQL transaction, so the rows stay locked -- and other drainers
        skip them -- until that transaction ends."""
        if limit is not None and limit < 0:
            limit = None
        try:
            self._ensure_sql_transaction()
            found = self._conn.execute(_SELECT_UNDELIVERED, (limit,)).fetchall()
        except psycopg.Error as exc:
            raise DriverError("cannot drain outbox: %s" % exc) from exc
        return [{"seq": row[0], "emission_id": row[1], "event": row[2],
                "payload": row[3], "created_at": row[4]}
               for row in found]

    def ack_outbox(self, seqs):
        """Mark every seq in `seqs` delivered (`delivered_at` = now, epoch
        milliseconds). Fails closed: if any seq names no row, DriverError
        before anything is written, so the known seqs in the batch stay
        undelivered too. A seq already delivered is a no-op success
        (idempotent re-ack).

        The check and the write share one transaction on this driver's own
        connection: its own BEGIN ... COMMIT outside a workflow transaction,
        the workflow's inside one -- never a side connection, which would
        wait on the row locks a drain in that transaction holds."""
        seqs = list(seqs)  # psycopg adapts a list as an array, a tuple not
        own = not self._in_transaction
        try:
            if own:
                self._conn.execute("BEGIN")
            else:
                self._ensure_sql_transaction()
            try:
                found = {row[0] for row in self._conn.execute(
                    _SELECT_OUTBOX_SEQS, (seqs,)).fetchall()}
                missing = [seq for seq in seqs if seq not in found]
                if missing:
                    raise DriverError("outbox ack: unknown seq(s): %s"
                                      % ", ".join(str(seq) for seq in missing))
                self._conn.execute(_MARK_DELIVERED,
                                   (int(time.time() * 1000), seqs))
            except Exception:
                if own:
                    self._conn.execute("ROLLBACK")
                raise
            if own:
                self._conn.execute("COMMIT")
            self._end_write()
        except psycopg.Error as exc:
            raise DriverError("cannot ack outbox: %s" % exc) from exc

    def idempotency_begin(self, workflow_id, key, now_ms, ttl_ms):
        """Claim `(workflow_id, key)` -- issue #113, r1. Two outcomes ask the
        caller to run nothing: `("in-progress", None)` (someone else owns
        this key right now -- 409) or `("done", (http_status, body))` (a
        prior run already finished -- replay it, whether it succeeded or
        failed). `("started", None)` means this call just claimed the key
        and the caller should run the workflow.

        Postgres: see the comment above `_CREATE_IDEMPOTENCY_TABLE` for which
        connection runs these statements.
        """
        try:
            with self._idempotency_connection() as conn:
                return self._claim_idempotency(conn, workflow_id, key,
                                               now_ms, ttl_ms)
        except psycopg.Error as exc:
            # Only borrowing the side connection lands here; every statement
            # in `_claim_idempotency` maps its own error.
            raise DriverError("cannot claim idempotency key %r: %s"
                              % (key, exc)) from exc

    def idempotency_finish(self, workflow_id, key, http_status, body):
        """Record the final disposition -- a SEPARATE statement, issued
        AFTER `run_workflow` returns (issue #113, r1). Deliberately outside
        that execution's own commit/rollback boundary: `run_workflow` calls
        `self.repo.rollback()` unconditionally on any failure, and writing
        this finalize step INSIDE that boundary would have a failed run's
        rollback undo it too -- reverting the row to `in-progress` forever,
        which blocks every future retry instead of replaying the failure
        (docs/serving.md's idempotency section)."""
        encoded = json.dumps(body)
        try:
            with self._idempotency_connection() as conn:
                conn.execute(_UPDATE_IDEMPOTENCY_DONE,
                             (http_status, encoded, workflow_id, key))
        except psycopg.Error as exc:
            raise DriverError("cannot record idempotency result for %r: %s"
                              % (key, exc)) from exc

    def idempotency_release(self, workflow_id, key):
        """Abandon an in-progress claim -- issue #118, D6 r2. A SEPARATE,
        immediately-committed DELETE (same style as `idempotency_begin`/
        `idempotency_finish` above), for exactly the outcome neither of
        those two covers: a run that ended in a state the caller wants
        RETRIED, not replayed and not left to self-heal via the TTL.

        A DELETE against a key with no row (already released, never
        claimed, or already finished) matches zero rows and raises
        nothing -- release is idempotent, a no-op on an absent claim.
        """
        try:
            with self._idempotency_connection() as conn:
                conn.execute(_DELETE_IDEMPOTENCY, (workflow_id, key))
        except psycopg.Error as exc:
            raise DriverError("cannot release idempotency key %r: %s"
                              % (key, exc)) from exc

    def close(self):
        conn, self._conn = getattr(self, "_conn", None), None
        if conn is not None:
            # The pool rolls back a connection returned inside a
            # transaction (an un-committed run) before lending it again.
            self._in_transaction = False
            self._sql_transaction_open = False
            self._pool.putconn(conn)

    # -- internals -------------------------------------------------------------

    @contextlib.contextmanager
    def _idempotency_connection(self):
        """The connection the idempotency statements run on: the driver's
        own (autocommit) one while no SQL transaction is open, otherwise a
        second one borrowed from the same pool, so the statement commits
        outside the open transaction and its `rollback()` cannot undo it."""
        if self._sql_transaction_open:
            with self._pool.connection() as side:
                yield side
        else:
            yield self._conn

    def _claim_idempotency(self, conn, workflow_id, key, now_ms, ttl_ms):
        try:
            conn.execute(_DELETE_EXPIRED_IDEMPOTENCY,
                         (workflow_id, key, now_ms - ttl_ms))
        except psycopg.Error as exc:
            raise DriverError("cannot clear expired idempotency key %r: %s"
                              % (key, exc)) from exc
        try:
            cursor = conn.execute(_INSERT_IDEMPOTENCY_CLAIM,
                                  (key, workflow_id, now_ms))
        except psycopg.Error as exc:
            raise DriverError("cannot claim idempotency key %r: %s"
                              % (key, exc)) from exc
        if cursor.rowcount == 1:
            return "started", None
        # The key has a row. Unlike sqlite (whose lost race answers
        # `in-progress` without reading), read it back: a `done` row is
        # replayed, which is what the contract asks for a recorded result.
        try:
            row = conn.execute(_SELECT_IDEMPOTENCY,
                               (workflow_id, key)).fetchone()
        except psycopg.Error as exc:
            raise DriverError("cannot read idempotency key %r: %s"
                              % (key, exc)) from exc
        if row is None or row[0] != "done":
            # A live claim owned by another request, or a row released
            # between the INSERT and this read -- either way this caller
            # must not run the workflow. No age check is needed: the
            # DELETE above already removed this key's expired row.
            return "in-progress", None
        status, http_status, body = row
        return "done", (http_status,
                        json.loads(body) if body is not None else None)

    def _read(self, entity_id, key):
        try:
            found = self._conn.execute(_SELECT_ROW, (entity_id, key)).fetchone()
        except psycopg.Error as exc:
            raise DriverError("cannot read %s: %s" % (entity_id, exc)) from exc
        if found is None:
            return None
        payload, version = found
        return _VersionedRow(payload, version)

    def _create(self, entity_id, key):
        try:
            self._ensure_sql_transaction()
            self._conn.execute(_INSERT_ROW,
                               (entity_id, key, _encode({"id": key})))
            self._end_write()
        except psycopg.errors.UniqueViolation as exc:
            raise ConflictError("repository create conflicts: %s already exists"
                                % entity_id) from exc
        except psycopg.Error as exc:
            raise DriverError("cannot create %s: %s" % (entity_id, exc)) from exc
        return {"affected": 1}

    def _touch(self, entity_id, operation, key):
        statement = _DELETE_ROW if operation == "delete" else _UPDATE_ROW
        try:
            if operation == "delete":
                self._ensure_sql_transaction()
                cursor = self._conn.execute(statement, (entity_id, key))
            else:
                # The read stays outside the transaction (still autocommit,
                # so still current) — only the write below opens it.
                current = self._read(entity_id, key)
                self._ensure_sql_transaction()
                cursor = self._conn.execute(
                    statement, (_encode(current if current is not None else {"id": key}),
                                entity_id, key))
            self._end_write()
        except psycopg.Error as exc:
            raise DriverError("cannot %s %s: %s" % (operation, entity_id, exc)) from exc
        if operation == "update" and cursor.rowcount > 0:
            # linkly#182 mirror: this UPDATE bumped `_version`; advance the
            # row this run already has bound for the key (if any) so its
            # next `persist` is not mistaken for a concurrent write.
            bound = self._bound_rows.get((entity_id, key))
            if bound is not None:
                bound.observed_version += 1
        return {"affected": cursor.rowcount if cursor.rowcount >= 0 else 0}
