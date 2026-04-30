import psycopg2
import time


def connect():
    return psycopg2.connect(
        host="database_service",
        database="database",
        user="app_user",
        password="123456"
    )


def connect_or_wait(retries=10, delay=2):
    connection = None
    for i in range(retries):
        try:
            connection = connect()
            return connection
        except Exception as e:
            print(f"DB not ready yet (attempt {i+1}/{retries})")
            time.sleep(delay)

    raise Exception("Could not connect to database after retries")


# TODO maybe move keys to a config file
def initialize_database(connection):
    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS games (
                id SERIAL PRIMARY KEY,
                event TEXT,
                result TEXT,
                white_elo INT,
                black_elo INT,
                eco TEXT,
                opening TEXT,
                time_control TEXT,
                termination TEXT
            );
        """)
    connection.commit()


def insert_games(connection, games: list[dict]):
    with connection.cursor() as cursor:
        cursor.executemany("""
            INSERT INTO games (
                event,
                result,
                white_elo,
                black_elo,
                eco, opening,
                time_control,
                termination
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, [
            (
                game.get("event"),
                game.get("result"),
                game.get("white_elo"),
                game.get("black_elo"),
                game.get("eco"),
                game.get("opening"),
                game.get("time_control"),
                game.get("termination"),
            )
            for game in games
        ])
    connection.commit()


def flush_games_batch(connection, batch):
    insert_games(connection, batch)
    print(f"Inserted game batch of length {len(batch)}")
    batch.clear()