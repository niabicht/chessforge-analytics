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


def connect_to_database_or_wait(retries=10, delay=0.5) -> psycopg2.extensions.connection:
    # Try to connect to the database, and if it fails, waits and retry for a specified number of times.
    connection = None
    for i in range(retries):
        try:
            connection = connect_to_database()
            return connection
        except Exception:
            print(f"DB not ready yet (attempt {i+1}/{retries})") # TODO refactor to log rules
            time.sleep(delay)

    raise Exception("Could not connect to database after retries")


def get_initialized_connection() -> psycopg2.extensions.connection:
    """Returns a database connection and ensures that the database schema is initialized."""    
    connection = connect_to_database_or_wait()
    schema.initialize_database(connection)

    return connection