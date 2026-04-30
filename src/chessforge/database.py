import psycopg2
import time

from chessforge.global_constants import GAME_COLUMNS, GAME_COLUMNS_SNAKE_CASE


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
    """If it does not exist, creates the games table, based on the GAME_COLUMNS definition."""
    columns_sql = ",\n".join(f"{GAME_COLUMNS_SNAKE_CASE[column]} {column_type}" for column, column_type in GAME_COLUMNS.items())
    with connection.cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS games (
                id SERIAL PRIMARY KEY,
                {columns_sql}
            );
        """)
    connection.commit()


def insert_games(connection, games: list[dict]):
    """Inserts a batch of games into the database, based on the GAME_COLUMNS definition."""
    columns = list(GAME_COLUMNS.keys())
    columns_str = ", ".join(GAME_COLUMNS_SNAKE_CASE[column] for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))

    with connection.cursor() as cursor:
        cursor.executemany(
            f"""
            INSERT INTO games ({columns_str})
            VALUES ({placeholders})
            """,
            [
                # loop over each column for each game, and create a tuple of values that fill the placeholders
                tuple(game.get(column) for column in columns)
                for game in games
            ]
        )
    connection.commit()


def flush_games_batch_into_database(connection, batch):
    insert_games(connection, batch)
    print(f"Inserted game batch of length {len(batch)}")
    batch.clear()