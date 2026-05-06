from typing import Any

import psycopg2
import psycopg2.extras

from chessforge.utils.global_constants import GAME_COLUMNS
from chessforge.utils.utils import mixed_to_snake


# NOTE could refactor into returning "struct"
def get_datasets_info(connection: psycopg2.extensions.connection) -> list[tuple[str, int, str]]:
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT name, game_count, ingested_at
            FROM datasets
            ORDER BY name DESC;
        """)
        
        return cursor.fetchall()
    

def does_dataset_exist(connection: psycopg2.extensions.connection, name: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM datasets WHERE name = %s;", (name,))
        return (cursor.fetchone() is not None)
    

def register_dataset_return_id(connection, name: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO datasets (name) VALUES (%s) RETURNING id;",
            (name,) # psycopg2 expects Tuple, here with one element
        )
        dataset_id = cursor.fetchone()[0]
    connection.commit()
    return dataset_id


def flush_games_batch_into_database(connection: psycopg2.extensions.connection, games: list[dict[str, Any]], dataset_id: int) -> None:
    """
    TODO
    """

    # NOTE: This uses executemany() for simplicity and readability.
    # Consider switching to PostgreSQL COPY for significantly faster bulk inserts if performance becomes an issue.
    # However, parsing currently takes about 7 times longer anyway (2026-05-06)

    # TODO comment
    columns = list(GAME_COLUMNS.keys())
    columns_snake = [mixed_to_snake(column) for column in columns]
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


def update_dataset_game_count(connection: psycopg2.extensions.connection, dataset_id: int, count: int) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE datasets SET game_count = %s WHERE id = %s;",
            (count, dataset_id)
        )
    connection.commit()


def delete_all_datasets(connection: psycopg2.extensions.connection):
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM games;")
        cursor.execute("DELETE FROM datasets;")
    connection.commit()


def delete_dataset(connection, dataset_name: str, log=lambda message: None) -> None:
    with connection.cursor() as cursor:
        # Find dataset
        cursor.execute("SELECT id FROM datasets WHERE name = %s;", (dataset_name,))
        result = cursor.fetchone()
        if not result:
            log(f"Dataset {dataset_name} not found.")
            return

        # Delete dataset from both the datasets and the games table
        dataset_id = result[0]
        cursor.execute("DELETE FROM games WHERE dataset_id = %s;", (dataset_id,)) # Must delete games first because games.dataset_id references datasets.id as foreign key.
        cursor.execute("DELETE FROM datasets WHERE id = %s;", (dataset_id,))

    connection.commit()
    log(f"Dataset {dataset_name} deleted.")


def execute_query_return_result(connection: psycopg2.extensions.connection, sql_query: str, params: dict = None) -> list[dict[str, Any]]:
    """
    TODO return lines as dicht with rows as keys
    """
    # Can later maybe be used with something like
    # params = {
    #     "min_elo": 1800,
    #     "max_elo": 2000
    # }
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor: # RealDictCursor returns query results as dicts keyed by column names
        cursor.execute(sql_query, params or ())
        return cursor.fetchall()