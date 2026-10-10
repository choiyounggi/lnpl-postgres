"""PostgreSQL RepositoryDriver — postgres mirror of lnpl's SqliteRepositoryDriver.

STATEMENT TEXT IS CONSTANT (lnpl.drivers module docstring): every SQL string
below is a literal; every value that varies rides in as a bound parameter.
"""

import json
import time

import psycopg
import psycopg.errors

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


class PostgresRepositoryDriver(RepositoryDriver):
    """A postgres-backed repository. One connection, always autocommit for
    reads; a SQL transaction is opened explicitly (`BEGIN`) only around a
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
        try:
            self._conn = psycopg.connect(dsn)
            self._conn.autocommit = True
            self._conn.execute(_CREATE_TABLE)
            self._conn.execute(_CREATE_OUTBOX_TABLE)
        except psycopg.Error as exc:
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

    def close(self):
        conn, self._conn = getattr(self, "_conn", None), None
        if conn is not None:
            conn.close()

    # -- internals -------------------------------------------------------------

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
