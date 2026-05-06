import psycopg2

from chessforge.utils.global_constants import GAME_COLUMNS
from chessforge.utils.utils import mixed_to_snake


def initialize_database(connection: psycopg2.extensions.connection) -> None:
    """
    TODO
    """

    # Determine the game table columns from single source of truth
    game_table_columns = ",\n".join(
        f"{mixed_to_snake(column)} {column_type}" # Stick to sql snake case convention
        for column, column_type in GAME_COLUMNS.items()
    )

    with connection.cursor() as cursor:
        # Initialize the table that keeps track of datasets from different files
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS datasets (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                game_count INT
            );
        """)

        # Initialize the table with the actual games
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS games (
                id SERIAL PRIMARY KEY,
                dataset_id INT REFERENCES datasets(id),
                {game_table_columns}
            );
        """)

    connection.commit()
