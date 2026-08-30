"""PostgreSQL RepositoryDriver factory for lnpl.drivers entry-point registration."""

__all__ = ["make_driver"]


def make_driver(arg):
    from .driver import PostgresRepositoryDriver

    return PostgresRepositoryDriver(dsn=arg)
