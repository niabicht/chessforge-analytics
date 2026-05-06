import time

import psycopg2

import chessforge.database.schema as schema


def connect_to_database() -> psycopg2.extensions.connection:
    # Connect to the PostgreSQL database using credentials corresponding to the docker-compose.yml file.
    return psycopg2.connect(
        host="database_service",
        database="chessforge_database",
        user="app_user",
        password="123456"
    )


def connect_to_database_or_wait(n_retries=10, delay=0.5) -> psycopg2.extensions.connection:
    # Try to connect to the database, and if it fails, waits and retry for a specified number of times.
    connection = None
    for i in range(n_retries):
        try:
            connection = connect_to_database()
            return connection
        except Exception:
            time.sleep(delay)

    raise Exception(f"Could not connect to database after {n_retries} retries.")


class InitializedConnection:
    """
    Context manager that opens a PostgreSQL connection and ensures the schema exists.

    On entry: Connects with retry logic (handles Docker startup race conditions).
    On exit: Closes the connection regardless of whether an exception occurred.

    Usage:
        with InitializedConnection() as connection:
            repository.some_function(connection, ...)
    """

    def __enter__(self) -> psycopg2.extensions.connection:
        self.connection = connect_to_database_or_wait()
        schema.initialize_database(self.connection)
        return self.connection

    def __exit__(self, _, __, ___) -> None:
        self.connection.close()