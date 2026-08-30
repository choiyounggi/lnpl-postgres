import unittest

from lnpl.drivers import DriverError, open_repository
from testcontainers.postgres import PostgresContainer

from lnpl_postgres import make_driver
from lnpl_postgres.driver import PostgresRepositoryDriver


class PostgresSPITest(unittest.TestCase):
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

    def test_open_repository_resolves_to_postgres_driver(self):
        driver = open_repository("postgres:" + self._dsn)
        self.addCleanup(driver.close)

        self.assertIsInstance(driver, PostgresRepositoryDriver)

    def test_empty_dsn_raises_value_error(self):
        with self.assertRaises(ValueError):
            make_driver("")

    def test_unreachable_dsn_raises_driver_error(self):
        with self.assertRaises(DriverError):
            make_driver("host=nope.invalid connect_timeout=2")


if __name__ == "__main__":
    unittest.main()
