import psycopg2
import time

from chessforge.global_constants import GAME_COLUMNS
from chessforge.utils import camel_to_snake


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


def initialize_database(connection):
    """Initializes database schema (datasets + games)."""

    # Dynamically generate game columns based on GAME_COLUMNS definition
    columns_sql = ",\n".join(
        f"{camel_to_snake(column)} {column_type}"
        for column, column_type in GAME_COLUMNS.items()
    )

    with connection.cursor() as cursor:
        # datasets table to keep track of different ingested datasets
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS datasets (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                game_count INT
            );
        """)

        # games table with datasets id foreign key
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS games (
                id SERIAL PRIMARY KEY,
                dataset_id INT REFERENCES datasets(id),
                {columns_sql}
            );
        """)

    connection.commit()


def get_initialized_connection():
    """Gets a database connection and ensures that the database schema is initialized."""
    connection = connect_to_database_or_wait()
    initialize_database(connection)
    return connection


# NOTE: This uses executemany() for simplicity and readability.
# For large-scale ingestion (e.g. full Lichess dumps), this can become a bottleneck.
# Consider switching to PostgreSQL COPY for significantly faster bulk inserts if performance becomes an issue.
def insert_games(connection, games: list[dict], dataset_id: int):
    columns = list(GAME_COLUMNS.keys())
    columns_snake = [camel_to_snake(column) for column in columns]

    columns_str = ", ".join(["dataset_id"] + columns_snake)
    placeholders = ", ".join(["%s"] * (len(columns) + 1))

    with connection.cursor() as cursor:
        cursor.executemany(
            f"""
            INSERT INTO games ({columns_str})
            VALUES ({placeholders})
            """,
            [
                (dataset_id, *tuple(game.get(column) for column in columns))
                for game in games
            ]
        )
    connection.commit()


def flush_games_batch_into_database(connection, batch, dataset_id):
    insert_games(connection, batch, dataset_id)
    batch.clear()


def dataset_exists(connection, name: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM datasets WHERE name = %s;", (name,))
        return cursor.fetchone() is not None


def create_dataset(connection, name: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO datasets (name) VALUES (%s) RETURNING id;",
            (name,)
        )
        dataset_id = cursor.fetchone()[0]
    connection.commit()
    return dataset_id


def update_dataset_game_count(connection, dataset_id: int, count: int):
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE datasets SET game_count = %s WHERE id = %s;",
            (count, dataset_id)
        )
    connection.commit()


def delete_dataset(connection, name: str):
    with connection.cursor() as cursor:
        cursor.execute("SELECT id FROM datasets WHERE name = %s;", (name,))
        result = cursor.fetchone()
        if not result: return False

        dataset_id = result[0]

        cursor.execute("DELETE FROM games WHERE dataset_id = %s;", (dataset_id,))
        cursor.execute("DELETE FROM datasets WHERE id = %s;", (dataset_id,))

    connection.commit()
    return True