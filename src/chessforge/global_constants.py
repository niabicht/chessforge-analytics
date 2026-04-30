from chessforge.utils import camel_to_snake

# Keys need to match the PGN headers, and values need to be either "TEXT" or "INT" depending on the type of data in that column. This is used for parsing the PGN files.
GAME_COLUMNS = {
    "Event": "TEXT",
    "Result": "TEXT",
    "WhiteElo": "INT",
    "BlackElo": "INT",
    "ECO": "TEXT",
    "Opening": "TEXT",
    "TimeControl": "TEXT",
    "Termination": "TEXT",
}

# Maps the PGN style CamelCase column names to snake_case for the database schema. For example, "WhiteElo" -> "white_elo". This is used when creating the database table and when inserting data, to ensure the column names in the SQL queries match the snake_case convention.  
GAME_COLUMNS_SNAKE_CASE = {
    column: camel_to_snake(column)
    for column in GAME_COLUMNS
}