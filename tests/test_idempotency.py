"""lnpl-postgres#8 (t1): the idempotency-claim methods on a real Postgres --
claim atomicity, replay, release, lazy per-key TTL expiry, and writes that
must survive the workflow transaction's rollback."""

import os
import threading
import unittest
import uuid
from unittest import mock

import psycopg
from lnpl.drivers import DriverError
from testcontainers.community.postgres import PostgresContainer

from lnpl_postgres import driver as driver_module
from lnpl_postgres.driver import PostgresRepositoryDriver

_TTL = 86400000


def _pool_env(**values):
    """Patch the pool settings env vars for the duration of a test."""
    env = {"LNPL_PG_POOL_" + name.upper(): str(value) for name, value in values.items()}
    return mock.patch.dict(os.environ, env)


class PostgresIdempotencyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._container = PostgresContainer("postgres:16")
        cls._container.start()
        cls._dsn = "postgresql://%s:%s@%s:%s/%s" % (
            cls._container.username,
            cls._container.password,
            cls._container.get_container_host_ip(),
            cls._container.get_exposed_port(5432),
            cls._container.dbname,
        )

    @classmethod
    def tearDownClass(cls):
        cls._container.stop()

    def setUp(self):
        self.admin_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        self.driver = self.open_driver()

    # -- helpers --------------------------------------------------------------

    def open_driver(self, dsn=None):
        driver = PostgresRepositoryDriver(dsn or self._dsn)
        self.addCleanup(driver.close)
        return driver

    def unique_dsn(self, lock_timeout_ms=None):
        """A DSN of its own (hence a pool of its own, built with the env in
        force at that moment); with `lock_timeout_ms`, every statement on
        that pool's connections gives up a lock wait after that long."""
        dsn = self._dsn + "?application_name=idem_" + uuid.uuid4().hex[:12]
        if lock_timeout_ms is not None:
            dsn += "&options=-c%20lock_timeout%3D" + str(lock_timeout_ms)
        return dsn

    def admin_execute(self, sql):
        with psycopg.connect(self._dsn, autocommit=True) as admin:
            admin.execute(sql)

    def admin_query(self, sql):
        with psycopg.connect(self._dsn, autocommit=True) as admin:
            return admin.execute(sql).fetchall()

    def fetch_rows(self):
        return self.admin_query(
            "SELECT key, status, http_status, body, created_at "
            "FROM lnpl_idempotency ORDER BY key")

    def claim(self, driver, key, now_ms=1000, ttl_ms=_TTL):
        return driver.idempotency_begin("wf", key, now_ms, ttl_ms)

    # -- capability and schema ------------------------------------------------

    def test_driver_declares_idempotency_and_not_outbox_drain(self):
        self.assertEqual((self.driver.supports_idempotency(),
                          self.driver.supports_outbox_drain()), (True, False))
        self.assertFalse(hasattr(self.driver, "drain_outbox"))
        self.assertFalse(hasattr(self.driver, "ack_outbox"))

    def test_two_drivers_create_the_table_once_with_the_reference_columns(self):
        self.open_driver()

        self.assertEqual(self.fetch_rows(), [])
        columns = {name: (data_type, nullable) for name, data_type, nullable in self.admin_query(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'lnpl_idempotency'")}
        self.assertEqual(columns, {
            "key": ("text", "NO"),
            "workflow_id": ("text", "NO"),
            "status": ("text", "NO"),
            "http_status": ("integer", "YES"),
            "body": ("text", "YES"),
            "created_at": ("bigint", "NO"),
        })
        primary_key = [row[0] for row in self.admin_query(
            "SELECT k.column_name FROM information_schema.table_constraints c "
            "JOIN information_schema.key_column_usage k "
            "ON k.constraint_name = c.constraint_name "
            "AND k.table_schema = c.table_schema AND k.table_name = c.table_name "
            "WHERE c.table_schema = 'public' AND c.table_name = 'lnpl_idempotency' "
            "AND c.constraint_type = 'PRIMARY KEY' ORDER BY k.ordinal_position")]
        self.assertEqual(primary_key, ["workflow_id", "key"])

    # -- claim, replay, release -----------------------------------------------

    def test_a_finished_claim_replays_its_status_and_body_in_key_order(self):
        body = {"zeta": 1, "alpha": {"b": 2, "a": 1}, "name": "한글"}
        self.assertEqual(self.claim(self.driver, "k3"), ("started", None))

        self.driver.idempotency_finish("wf", "k3", 201, body)

        status, stored = self.claim(self.driver, "k3")
        self.assertEqual(status, "done")
        self.assertEqual(stored, (201, body))
        self.assertEqual(list(stored[1]), ["zeta", "alpha", "name"])

    def test_a_finished_claim_with_a_null_body_replays_none(self):
        self.assertEqual(self.claim(self.driver, "k4"), ("started", None))

        self.driver.idempotency_finish("wf", "k4", 204, None)

        self.assertEqual(self.claim(self.driver, "k4"), ("done", (204, None)))

    def test_a_second_claim_on_a_live_key_is_in_progress(self):
        self.assertEqual(self.claim(self.driver, "k5"), ("started", None))

        self.assertEqual(self.claim(self.driver, "k5"), ("in-progress", None))
        self.assertEqual(self.fetch_rows(), [("k5", "in-progress", None, None, 1000)])

    def test_release_frees_the_key_and_is_a_no_op_on_an_absent_key(self):
        self.driver.idempotency_release("wf", "never-claimed")
        self.driver.idempotency_release("wf", "never-claimed")
        self.assertEqual(self.fetch_rows(), [])

        self.assertEqual(self.claim(self.driver, "k6"), ("started", None))
        self.driver.idempotency_release("wf", "k6")
        self.assertEqual(self.claim(self.driver, "k6"), ("started", None))

        self.assertEqual(self.claim(self.driver, "k6b"), ("started", None))
        self.driver.idempotency_finish("wf", "k6b", 200, {"ok": True})
        self.driver.idempotency_release("wf", "k6b")
        self.assertEqual(self.claim(self.driver, "k6b"), ("started", None))

    def test_finish_without_a_claim_records_nothing(self):
        self.driver.idempotency_finish("wf", "k7", 200, {"ok": True})

        self.assertEqual(self.fetch_rows(), [])
        self.assertEqual(self.claim(self.driver, "k7"), ("started", None))

    # -- expiry -----------------------------------------------------------------

    def test_a_claim_expires_exactly_when_its_age_reaches_the_ttl(self):
        self.assertEqual(self.claim(self.driver, "k8", 1000, 500), ("started", None))

        self.assertEqual(self.claim(self.driver, "k8", 1499, 500), ("in-progress", None))
        self.assertEqual(self.claim(self.driver, "k8", 1500, 500), ("started", None))
        self.assertEqual(self.admin_query("SELECT created_at FROM lnpl_idempotency"),
                         [(1500,)])

    def test_an_expired_finished_claim_is_swept_and_claimed_afresh(self):
        self.assertEqual(self.claim(self.driver, "k9", 1000, 500), ("started", None))
        self.driver.idempotency_finish("wf", "k9", 201, {"id": "x"})

        self.assertEqual(self.claim(self.driver, "k9", 1500, 500), ("started", None))
        self.assertEqual(self.fetch_rows(), [("k9", "in-progress", None, None, 1500)])

    # -- concurrency --------------------------------------------------------------

    def test_concurrent_claims_on_one_key_start_exactly_one_run(self):
        other = self.open_driver()
        errors = []
        for n in range(10):
            key = "k10-%d" % n
            barrier = threading.Barrier(2)
            results = []

            def run(driver, key=key, barrier=barrier, results=results):
                try:
                    barrier.wait(timeout=10)
                    results.append(self.claim(driver, key))
                except Exception as exc:  # surfaced by the assert below
                    errors.append(exc)

            threads = [threading.Thread(target=run, args=(driver,))
                       for driver in (self.driver, other)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=30)

            self.assertEqual(errors, [])
            self.assertEqual(sorted(results),
                             [("in-progress", None), ("started", None)])

    def test_a_conflicting_row_gone_before_read_back_answers_in_progress(self):
        self.assertEqual(self.claim(self.driver, "k11"), ("started", None))
        self.driver.idempotency_finish("wf", "k11", 200, {"ok": True})
        self.assertEqual(self.claim(self.driver, "k11"), ("done", (200, {"ok": True})))
        vanished = ("SELECT status, http_status, body FROM lnpl_idempotency "
                    "WHERE false AND workflow_id = %s AND key = %s")

        with mock.patch.object(driver_module, "_SELECT_IDEMPOTENCY", vanished):
            self.assertEqual(self.claim(self.driver, "k11"), ("in-progress", None))

    # -- writes that must survive the workflow transaction ------------------------

    def test_finish_inside_an_open_workflow_transaction_survives_its_rollback(self):
        self.assertEqual(self.claim(self.driver, "k12"), ("started", None))
        self.driver.begin()
        self.driver.execute("widget", "create", "w1")
        self.assertTrue(self.driver._sql_transaction_open)

        self.driver.idempotency_finish("wf", "k12", 200, {"ok": True})
        self.driver.rollback()

        later = self.open_driver()
        self.assertEqual(self.claim(later, "k12"), ("done", (200, {"ok": True})))
        self.assertIsNone(later.execute("widget", "read", "w1"))

    def test_release_inside_an_open_workflow_transaction_survives_its_rollback(self):
        self.assertEqual(self.claim(self.driver, "k13"), ("started", None))
        self.driver.begin()
        self.driver.execute("widget", "create", "w1")
        self.assertTrue(self.driver._sql_transaction_open)

        self.driver.idempotency_release("wf", "k13")
        self.driver.rollback()

        later = self.open_driver()
        self.assertEqual(self.claim(later, "k13"), ("started", None))
        self.assertIsNone(later.execute("widget", "read", "w1"))

    def test_a_claim_inside_an_open_workflow_transaction_is_visible_at_once(self):
        # lock_timeout: if the claim ever ran inside the open transaction,
        # `other` would block on it; this turns that hang into an error.
        other = self.open_driver(self.unique_dsn(lock_timeout_ms=2000))
        self.driver.begin()
        self.driver.execute("widget", "create", "w1")
        self.assertTrue(self.driver._sql_transaction_open)

        self.assertEqual(self.claim(self.driver, "k14"), ("started", None))
        self.assertEqual(self.claim(other, "k14"), ("in-progress", None))
        self.driver.rollback()
        self.assertEqual(self.claim(other, "k14"), ("in-progress", None))

    def test_without_a_sql_transaction_the_drivers_own_connection_is_used(self):
        # One-connection pool: a side lend would time out, so success here
        # proves every call ran on the driver's own connection.
        with _pool_env(min_size=1, max_size=1, timeout=1):
            driver = self.open_driver(self.unique_dsn())
        self.assertEqual(self.claim(driver, "k15"), ("started", None))
        driver.begin()
        self.assertFalse(driver._sql_transaction_open)

        driver.idempotency_finish("wf", "k15", 200, {"ok": True})
        driver.commit()

        self.assertEqual(self.claim(driver, "k15"), ("done", (200, {"ok": True})))
        driver.idempotency_release("wf", "k15")
        self.assertEqual(self.claim(driver, "k15"), ("started", None))

    # -- errors -------------------------------------------------------------------

    def test_finish_inside_a_transaction_on_a_one_connection_pool_raises_driver_error(self):
        with _pool_env(min_size=1, max_size=1, timeout=1):
            driver = self.open_driver(self.unique_dsn())
        self.assertEqual(self.claim(driver, "k16"), ("started", None))
        driver.begin()
        driver.execute("widget", "create", "w1")
        self.assertTrue(driver._sql_transaction_open)

        with self.assertRaises(DriverError) as ctx:
            driver.idempotency_finish("wf", "k16", 200, {"ok": True})

        self.assertTrue(str(ctx.exception).startswith(
            "cannot record idempotency result for 'k16'"), str(ctx.exception))
        driver.rollback()
        self.assertEqual(self.claim(driver, "k16"), ("in-progress", None))

    def test_database_errors_surface_as_driver_error_without_the_dsn(self):
        self.admin_execute("DROP TABLE lnpl_idempotency")
        cases = [
            (lambda: self.claim(self.driver, "k"),
             "cannot clear expired idempotency key 'k'"),
            (lambda: self.driver.idempotency_finish("wf", "k", 200, {"ok": True}),
             "cannot record idempotency result for 'k'"),
            (lambda: self.driver.idempotency_release("wf", "k"),
             "cannot release idempotency key 'k'"),
        ]
        for call, prefix in cases:
            with self.subTest(prefix=prefix):
                with self.assertRaises(DriverError) as ctx:
                    call()
                message = str(ctx.exception)
                self.assertTrue(message.startswith(prefix), message)
                self.assertIsInstance(ctx.exception.__cause__, psycopg.Error)
                self.assertNotIn(self._dsn, message)

    def test_a_key_postgres_cannot_store_raises_driver_error(self):
        with self.assertRaises(DriverError) as ctx:
            self.claim(self.driver, "bad\x00key")

        message = str(ctx.exception)
        self.assertTrue(message.startswith(
            "cannot clear expired idempotency key 'bad\\x00key'"), message)
        self.assertIsInstance(ctx.exception.__cause__, psycopg.DataError)
        self.assertEqual(self.fetch_rows(), [])


if __name__ == "__main__":
    unittest.main()
