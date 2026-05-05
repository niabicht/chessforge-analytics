from chessforge.utils.utils import camel_to_snake
from chessforge.utils.global_constants import GAME_COLUMNS


def initialize_database(connection):
    columns_sql = ",\n".join(
        f"{camel_to_snake(column)} {column_type}"
        for column, column_type in GAME_COLUMNS.items()
    )

    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS datasets (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                game_count INT
            );
        """)

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS games (
                id SERIAL PRIMARY KEY,
                dataset_id INT REFERENCES datasets(id),
                {columns_sql}
            );
        """)

    connection.commit()
