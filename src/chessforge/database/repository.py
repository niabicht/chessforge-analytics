from psycopg2.extras import RealDictCursor

from chessforge.utils.global_constants import GAME_COLUMNS
from chessforge.utils.utils import camel_to_snake


# NOTE could refactor into returning "struct"
def get_datasets_info(connection) -> list[tuple[str, int, str]]:
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT name, game_count, ingested_at
            FROM datasets
            ORDER BY ingested_at DESC;
        """)
        
        return cursor.fetchall()
    
def does_dataset_exist(connection, name: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM datasets WHERE name = %s;", (name,))
        return (cursor.fetchone() is not None)
    
def create_dataset(connection, name: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO datasets (name) VALUES (%s) RETURNING id;",
            (name,)
        )
        dataset_id = cursor.fetchone()[0]
    connection.commit()
    return dataset_id

def flush_games_batch_into_database(connection, games: list[dict], dataset_id: int):
    # NOTE: This uses executemany() for simplicity and readability.
    # For large-scale ingestion (e.g. full Lichess dumps), this can become a bottleneck.
    # Consider switching to PostgreSQL COPY for significantly faster bulk inserts if performance becomes an issue.
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
            # for each game, insert values for placeholders, with dataset_id as the first value followed by game values in the correct order
            [
                (dataset_id, *tuple(game.get(column) for column in columns))
                for game in games
            ]
        )
    connection.commit()
    games.clear()

def update_dataset_game_count(connection, dataset_id: int, count: int):
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE datasets SET game_count = %s WHERE id = %s;",
            (count, dataset_id)
        )
    connection.commit()

def delete_all_datasets(connection):
    
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM games;")
        cursor.execute("DELETE FROM datasets;")
    connection.commit()

def delete_dataset(connection, dataset_name: str, log=lambda _: None) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT id FROM datasets WHERE name = %s;", (dataset_name,))
        result = cursor.fetchone()
        if not result:
            log(f"Dataset {dataset_name} not found.")
            return

        dataset_id = result[0]
        cursor.execute("DELETE FROM games WHERE dataset_id = %s;", (dataset_id,))
        cursor.execute("DELETE FROM datasets WHERE id = %s;", (dataset_id,))

    connection.commit()
    log(f"Dataset {dataset_name} deleted.")

def execute_query(connection, sql_query: str):
    with connection.cursor() as cursor:
        cursor.execute(sql_query)
        results = cursor.fetchall()
    return results

def execute_query(connection, sql_query: str, params: dict = None):
    # can later maybe be used with something like
    # params = {
    #     "min_elo": 1800,
    #     "max_elo": 2000
    # }
    with connection.cursor(cursor_factory=RealDictCursor) as cursor: # RealDictCursor returns query results as dicts keyed by column names
        cursor.execute(sql_query, params or ())
        return cursor.fetchall()