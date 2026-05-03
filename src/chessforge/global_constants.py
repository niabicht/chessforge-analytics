# TODO maybe move these constants to a config file?
LICHESS_PGN_FILE_NAME_PREFIX = "lichess_db_standard_rated"
EXAMPLE_FILE_NAME = "example_games"
PATH_DATA_RAW = "data/raw/"
PATH_EXAMPLE_FILE = f"data/example/{EXAMPLE_FILE_NAME}.pgn.zst"

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