"""Issue #4: drivers borrow from one connection pool per DSN instead of
opening a new connection each, so `lnpl serve` (one driver per request)
keeps the server's connection count bounded by the pool's max size."""

import os
import threading
import time
import unittest
import uuid
from unittest import mock

import psycopg
import psycopg_pool
from lnpl.drivers import DriverError
from testcontainers.postgres import PostgresContainer

from lnpl_postgres import driver as driver_module
from lnpl_postgres.driver import PostgresRepositoryDriver


def _pool_env(**values):
    """Patch the pool settings env vars for the duration of a test."""
    env = {"LNPL_PG_POOL_" + name.upper(): str(value) for name, value in values.items()}
    return mock.patch.dict(os.environ, env)


class PostgresPoolTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._container = PostgresContainer("postgres:16")
        cls._container.start()
        cls._base_dsn = "postgresql://%s:%s@%s:%s/%s" % (
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
        # A fresh application_name gives every test its own DSN, hence its
        # own pool with this test's settings, and lets pg_stat_activity
        # count exactly this test's server connections.
        self.app = "pooltest_%s" % uuid.uuid4().hex[:12]
        self.dsn = self._base_dsn + "?application_name=" + self.app

    def open_driver(self):
        driver = PostgresRepositoryDriver(self.dsn)
        self.addCleanup(driver.close)
        return driver

    def server_connections(self):
        with psycopg.connect(self._base_dsn, autocommit=True) as admin:
            return admin.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = %s",
                (self.app,)).fetchone()[0]

    def backend_pid(self, driver):
        return driver._conn.execute("SELECT pg_backend_pid()").fetchone()[0]

    # -- normal ---------------------------------------------------------------

    def test_a_closed_driver_hands_its_connection_to_the_next_one(self):
        with _pool_env(min_size=1, max_size=1):
            first = PostgresRepositoryDriver(self.dsn)
            pid = self.backend_pid(first)
            first.close()

            second = self.open_driver()

            self.assertEqual(self.backend_pid(second), pid)

    def test_many_drivers_in_a_row_reuse_one_server_connection(self):
        with _pool_env(min_size=1, max_size=1):
            pids = set()
            for n in range(20):
                driver = PostgresRepositoryDriver(self.dsn)
                driver.execute("widget", "create", "w%d" % n)
                pids.add(self.backend_pid(driver))
                driver.close()

            # Unpooled, every driver was a new backend: 20 distinct pids.
            self.assertEqual(len(pids), 1)
            self.assertEqual(self.server_connections(), 1)

    def test_concurrent_drivers_never_exceed_max_size(self):
        with _pool_env(min_size=1, max_size=3, timeout=30):
            peak = []
            errors = []
            started = threading.Barrier(12)

            def request(n):
                try:
                    started.wait()  # all 12 ask for a connection at once
                    driver = PostgresRepositoryDriver(self.dsn)
                    try:
                        driver.execute("widget", "create", "c%d" % n)
                        time.sleep(0.3)  # hold it, so borrowers overlap
                        peak.append(self.server_connections())
                    finally:
                        driver.close()
                except Exception as exc:  # pragma: no cover - reported below
                    errors.append(exc)

            threads = [threading.Thread(target=request, args=(n,)) for n in range(12)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            self.assertEqual(errors, [])
            self.assertEqual(len(peak), 12)
            # Reached the cap (so the overlap was real) and never passed it.
            self.assertEqual(max(peak), 3)

    def test_a_connection_returned_mid_transaction_is_rolled_back(self):
        with _pool_env(min_size=1, max_size=1):
            first = PostgresRepositoryDriver(self.dsn)
            first.begin()
            first.execute("widget", "create", "uncommitted")
            first.close()  # never committed

            second = self.open_driver()

            self.assertIsNone(second.execute("widget", "read", "uncommitted"))
            self.assertEqual(second._conn.info.transaction_status,
                             psycopg.pq.TransactionStatus.IDLE)

    # -- error ----------------------------------------------------------------

    def test_an_exhausted_pool_fails_with_a_driver_error_after_the_timeout(self):
        with _pool_env(min_size=1, max_size=1, timeout=1):
            self.open_driver()  # holds the only connection
            started = time.monotonic()

            with self.assertRaises(DriverError) as ctx:
                PostgresRepositoryDriver(self.dsn)

            self.assertIn("all 1 pooled connections stayed in use", str(ctx.exception))
            self.assertNotIn(self._container.password, str(ctx.exception))
            self.assertLess(time.monotonic() - started, 10)

    def test_a_timeout_with_free_slots_is_reported_as_unreachable(self):
        with _pool_env(min_size=1, max_size=2, timeout=1):
            self.open_driver()  # builds the pool for this DSN
            pool = driver_module._POOLS[self.dsn][1]
            # The server is gone: no connection is handed out, none is in use.
            with mock.patch.object(pool, "getconn", side_effect=psycopg_pool.PoolTimeout("t")), \
                    mock.patch.object(pool, "get_stats",
                                      return_value={"pool_size": 0, "pool_available": 0}):
                with self.assertRaises(DriverError) as ctx:
                    PostgresRepositoryDriver(self.dsn)

            self.assertIn("could not be reached", str(ctx.exception))

    def test_a_pool_built_by_another_process_is_not_reused(self):
        with _pool_env(min_size=1, max_size=1):
            self.open_driver()
            inherited = driver_module._POOLS[self.dsn][1]
            # Pretend this process is a fork(): the entry's owner pid differs.
            driver_module._POOLS[self.dsn] = (-1, inherited)

            fresh = driver_module._pool_for(self.dsn)
            self.addCleanup(fresh.close)

            self.assertIsNot(fresh, inherited)
            self.assertEqual(driver_module._POOLS[self.dsn], (os.getpid(), fresh))

    def test_concurrent_first_use_builds_one_pool_per_dsn(self):
        with _pool_env(min_size=0, max_size=4):
            pools = []
            started = threading.Barrier(6)

            def first_use():
                started.wait()
                pools.append(driver_module._pool_for(self.dsn))

            threads = [threading.Thread(target=first_use) for _ in range(6)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            self.assertEqual(len(pools), 6)
            self.assertEqual(len({id(p) for p in pools}), 1)
            self.assertIs(pools[0], driver_module._POOLS[self.dsn][1])

    def test_a_waiting_driver_gets_the_connection_once_it_is_returned(self):
        with _pool_env(min_size=1, max_size=1, timeout=10):
            holder = PostgresRepositoryDriver(self.dsn)
            threading.Timer(0.5, holder.close).start()

            waiter = self.open_driver()

            self.assertIsNone(waiter.execute("widget", "read", "absent"))

    def test_invalid_pool_settings_are_refused(self):
        cases = {
            "max_size zero": dict(max_size=0),
            "min above max": dict(min_size=5, max_size=2),
            "not a number": dict(max_size="many"),
            "timeout zero": dict(timeout=0),
            "negative min": dict(min_size=-1),
        }
        for label, values in cases.items():
            with self.subTest(case=label):
                self.setUp()  # a fresh DSN, so a fresh pool is built
                with _pool_env(**values):
                    with self.assertRaises(ValueError):
                        PostgresRepositoryDriver(self.dsn)

    # -- boundary ---------------------------------------------------------------

    def test_close_twice_returns_the_connection_once(self):
        with _pool_env(min_size=1, max_size=1, timeout=1):
            driver = PostgresRepositoryDriver(self.dsn)
            driver.close()
            driver.close()

            again = self.open_driver()

            self.assertIsNone(again.execute("widget", "read", "absent"))

    def test_min_size_zero_still_serves(self):
        with _pool_env(min_size=0, max_size=1):
            driver = self.open_driver()

            self.assertEqual(driver.execute("widget", "create", "z"), {"affected": 1})


if __name__ == "__main__":
    unittest.main()
