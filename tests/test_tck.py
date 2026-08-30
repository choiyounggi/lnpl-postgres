import unittest

import psycopg
from lnpl.testing import RepositoryDriverTCK
from testcontainers.postgres import PostgresContainer

from lnpl_postgres.driver import PostgresRepositoryDriver


class PostgresTCKTest(RepositoryDriverTCK, unittest.TestCase):
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
        admin = psycopg.connect(self._dsn)
        admin.autocommit = True
        admin.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        admin.close()
        super().setUp()

    def make_driver(self):
        return PostgresRepositoryDriver(self._dsn)


if __name__ == "__main__":
    unittest.main()
