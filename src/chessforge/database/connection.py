import psycopg2
import time

from chessforge.database.schema import initialize_database


def connect_to_database():
    """Connects to the PostgreSQL database using credentials defined in the docker-compose.yml."""
    return psycopg2.connect(
        host="database_service",
        database="database",
        user="app_user",
        password="123456"
    )


def connect_to_database_or_wait(retries=10, delay=2):
    """Tries to connect to the database, and if it fails, waits and retries for a specified number of times."""
    connection = None
    for i in range(retries):
        try:
            connection = connect_to_database()
            return connection
        except Exception as e:
            print(f"DB not ready yet (attempt {i+1}/{retries})")
            time.sleep(delay)

    raise Exception("Could not connect to database after retries")


def get_initialized_connection():
    """Gets a database connection and ensures that the database schema is initialized."""
    connection = connect_to_database_or_wait()
    initialize_database(connection)

    return connection