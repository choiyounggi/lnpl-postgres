"""lnpl-postgres#8 (t2): drain_outbox/ack_outbox on a real Postgres -- seq
order, the read_outbox row shape, at-least-once redelivery, SKIP LOCKED
between open transactions, idempotent re-ack, all-or-nothing rejection."""

import os
import time
import unittest
import uuid
from unittest import mock

import psycopg
from lnpl.drivers import DriverError
from testcontainers.community.postgres import PostgresContainer

from lnpl_postgres.driver import PostgresRepositoryDriver

_EVENT = "entity.event.t2"


def _pool_env(**values):
    """Patch the pool settings env vars for the duration of a test."""
    env = {"LNPL_PG_POOL_" + name.upper(): str(value) for name, value in values.items()}
    return mock.patch.dict(os.environ, env)


class PostgresOutboxDrainTest(unittest.TestCase):
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
        dsn = self._dsn + "?application_name=outbox_" + uuid.uuid4().hex[:12]
        if lock_timeout_ms is not None:
            dsn += "&options=-c%20lock_timeout%3D" + str(lock_timeout_ms)
        return dsn

    def admin(self):
        """An autocommit side connection whose statements give up a lock
        wait after 2 s, so a row lock a regression leaks (an ack that never
        commits) fails the test with LockNotAvailable instead of hanging."""
        return psycopg.connect(self._dsn, autocommit=True,
                               options="-c lock_timeout=2000")

    def admin_execute(self, sql, params=None):
        with self.admin() as admin:
            admin.execute(sql, params)

    def admin_query(self, sql):
        with self.admin() as admin:
            return admin.execute(sql).fetchall()

    def emit(self, n, driver=None):
        (driver or self.driver).record_emission({
            "emission_id": "t2.emit#%d" % n, "event": _EVENT,
            "payload": {"x": n}})

    def seqs(self):
        return [row[0] for row in self.admin_query(
            "SELECT seq FROM lnpl_outbox ORDER BY seq")]

    def delivered(self):
        return dict(self.admin_query(
            "SELECT seq, delivered_at FROM lnpl_outbox ORDER BY seq"))

    # -- capability -------------------------------------------------------------

    def test_driver_declares_outbox_drain_and_idempotency(self):
        self.assertEqual((self.driver.supports_idempotency(),
                          self.driver.supports_outbox_drain()), (True, True))
        self.assertNotIn("supports_outbox_drain", vars(PostgresRepositoryDriver))
        self.assertNotIn("supports_idempotency", vars(PostgresRepositoryDriver))

    # -- drain ------------------------------------------------------------------

    def test_draining_an_empty_outbox_returns_an_empty_list(self):
        self.assertEqual(self.driver.drain_outbox(), [])
        self.assertEqual(self.driver.drain_outbox(limit=1), [])

    def test_drain_lists_undelivered_rows_oldest_first_in_the_read_outbox_shape(self):
        self.emit(1)
        self.emit(2)
        s1, s2 = self.seqs()
        # A no-op UPDATE writes s1's new row version after s2 in the heap, so
        # a scan in physical order would list s2 first; only ORDER BY seq
        # keeps s1 first.
        self.admin_execute(
            "UPDATE lnpl_outbox SET payload = payload WHERE seq = %s", (s1,))

        rows = self.driver.drain_outbox()

        self.assertEqual(rows, self.driver.read_outbox(_EVENT))
        self.assertEqual([row["seq"] for row in rows], [s1, s2])
        for row in rows:
            self.assertEqual(set(row), {"seq", "emission_id", "event",
                                        "payload", "created_at"})
        self.assertEqual([row["payload"] for row in rows], [{"x": 1}, {"x": 2}])
        self.assertEqual([row["emission_id"] for row in rows],
                         ["t2.emit#1", "t2.emit#2"])

    def test_acked_rows_never_drain_again_on_any_driver_but_stay_in_read_outbox(self):
        self.emit(1)
        self.emit(2)
        s1, s2 = self.seqs()
        before = int(time.time() * 1000)

        self.driver.ack_outbox([s1, s2])

        after = int(time.time() * 1000)
        other = self.open_driver()
        self.assertEqual(other.drain_outbox(), [])
        self.assertEqual([row["seq"] for row in other.read_outbox(_EVENT)], [s1, s2])
        delivered = self.delivered()
        for seq in (s1, s2):
            self.assertIsNotNone(delivered[seq])
            self.assertLessEqual(before, delivered[seq])
            self.assertLessEqual(delivered[seq], after)
        # The ack's own BEGIN ... COMMIT left the connection idle.
        self.assertEqual(self.driver._conn.info.transaction_status,
                         psycopg.pq.TransactionStatus.IDLE)

    def test_draining_twice_without_an_ack_lists_the_same_rows_and_holds_no_lock(self):
        self.emit(1)
        self.emit(2)
        s1, s2 = self.seqs()

        first = self.driver.drain_outbox()

        # NOWAIT raises LockNotAvailable at once if the drain left a row lock.
        self.assertEqual(self.admin_query(
            "SELECT seq FROM lnpl_outbox WHERE delivered_at IS NULL "
            "ORDER BY seq FOR UPDATE NOWAIT"), [(s1,), (s2,)])
        second = self.open_driver().drain_outbox()
        self.assertEqual(first, second)
        self.assertEqual([row["seq"] for row in first], [s1, s2])
        self.assertFalse(self.driver._sql_transaction_open)

    def test_limit_caps_the_batch_from_the_lowest_seq(self):
        for n in (1, 2, 3):
            self.emit(n)
        s1, s2, s3 = self.seqs()

        def drained(**kwargs):
            return [row["seq"] for row in self.driver.drain_outbox(**kwargs)]

        self.assertEqual(drained(limit=1), [s1])
        self.assertEqual(drained(limit=0), [])
        self.assertEqual(drained(limit=5), [s1, s2, s3])
        self.assertEqual(drained(limit=None), [s1, s2, s3])
        self.assertEqual(drained(limit=-1), [s1, s2, s3])
        self.assertEqual(drained(), [s1, s2, s3])

    def test_two_drainers_in_open_transactions_receive_disjoint_rows(self):
        for n in (1, 2, 3):
            self.emit(n)
        s1, s2, s3 = self.seqs()
        # lock_timeout: a drainer that waits on the other's row locks fails
        # after 2 s instead of hanging the run.
        d1 = self.open_driver(self.unique_dsn(lock_timeout_ms=2000))
        d2 = self.open_driver(self.unique_dsn(lock_timeout_ms=2000))

        d1.begin()
        a = d1.drain_outbox(limit=2)
        self.assertTrue(d1._sql_transaction_open)
        d2.begin()
        b = d2.drain_outbox()

        self.assertEqual([row["seq"] for row in a], [s1, s2])
        self.assertEqual([row["seq"] for row in b], [s3])
        # Outside a transaction every locked row is skipped.
        self.assertEqual(self.driver.drain_outbox(), [])
        d1.rollback()
        d2.commit()
        self.assertEqual([row["seq"] for row in self.driver.drain_outbox()],
                         [s1, s2, s3])

    def test_drain_and_ack_inside_an_open_workflow_transaction_share_its_connection(self):
        # One-connection pool: a side-connection borrow fails within 1 s;
        # lock_timeout: a wait on this transaction's own row locks fails in 2 s.
        with _pool_env(min_size=1, max_size=1, timeout=1):
            driver = self.open_driver(self.unique_dsn(lock_timeout_ms=2000))
        self.emit(1)
        self.emit(2)
        s1, s2 = self.seqs()
        driver.begin()
        driver.execute("widget", "create", "w1")
        self.assertTrue(driver._sql_transaction_open)

        rows = driver.drain_outbox()
        driver.ack_outbox([rows[0]["seq"]])

        self.assertEqual([row["seq"] for row in driver.drain_outbox()], [s2])
        driver.rollback()
        self.assertEqual([row["seq"] for row in self.driver.drain_outbox()], [s1, s2])
        self.assertEqual(self.delivered(), {s1: None, s2: None})

    # -- ack --------------------------------------------------------------------

    def test_re_acking_a_delivered_seq_is_an_idempotent_success_that_keeps_its_time(self):
        self.emit(1)
        self.emit(2)
        s1, s2 = self.seqs()
        self.driver.ack_outbox([s1])
        # Stand in for an earlier first ack, so an overwrite would show.
        self.admin_execute(
            "UPDATE lnpl_outbox SET delivered_at = 1 WHERE seq = %d" % s1)

        self.driver.ack_outbox([s1])

        self.assertEqual(self.delivered()[s1], 1)
        self.driver.ack_outbox([s1, s2])
        delivered = self.delivered()
        self.assertEqual(delivered[s1], 1)
        self.assertIsNotNone(delivered[s2])
        self.assertEqual(self.driver.drain_outbox(), [])

    def test_an_unknown_seq_rejects_the_whole_batch_and_marks_nothing(self):
        self.emit(1)
        self.emit(2)
        s1, s2 = self.seqs()
        missing = max(self.seqs()) + 1000

        with self.assertRaises(DriverError) as ctx:
            self.driver.ack_outbox([s1, missing, s2, missing + 1])

        self.assertEqual(str(ctx.exception), "outbox ack: unknown seq(s): %d, %d"
                         % (missing, missing + 1))
        self.assertIsNone(ctx.exception.__cause__)
        self.assertEqual(self.delivered(), {s1: None, s2: None})
        # Rolled back, not left open.
        self.assertEqual(self.driver._conn.info.transaction_status,
                         psycopg.pq.TransactionStatus.IDLE)
        self.assertEqual([row["seq"] for row in self.driver.drain_outbox()], [s1, s2])

    def test_an_empty_or_tuple_seq_batch_binds_as_one_array(self):
        self.emit(1)
        self.emit(2)
        s1, s2 = self.seqs()

        self.driver.ack_outbox([])

        self.assertEqual(self.delivered(), {s1: None, s2: None})
        self.driver.ack_outbox((s1,))
        delivered = self.delivered()
        self.assertIsNotNone(delivered[s1])
        self.assertIsNone(delivered[s2])
        self.assertEqual([row["seq"] for row in self.driver.drain_outbox()], [s2])
        self.driver.ack_outbox([s2, s2])  # a repeated known seq is legal
        self.assertIsNotNone(self.delivered()[s2])
        self.assertEqual(self.driver.drain_outbox(), [])

    # -- errors -------------------------------------------------------------------

    def test_database_errors_surface_as_driver_error_without_the_dsn(self):
        self.admin_execute("DROP TABLE lnpl_outbox")
        cases = [
            (lambda: self.driver.drain_outbox(), "cannot drain outbox: "),
            (lambda: self.driver.ack_outbox([1]), "cannot ack outbox: "),
        ]
        for call, prefix in cases:
            with self.subTest(prefix=prefix):
                with self.assertRaises(DriverError) as ctx:
                    call()
                message = str(ctx.exception)
                self.assertTrue(message.startswith(prefix), message)
                self.assertIsInstance(ctx.exception.__cause__, psycopg.Error)
                self.assertNotIn(self._dsn, message)
                # A failed ack rolled its own transaction back.
                self.assertEqual(self.driver._conn.info.transaction_status,
                                 psycopg.pq.TransactionStatus.IDLE)


if __name__ == "__main__":
    unittest.main()
