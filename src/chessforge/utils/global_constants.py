PATH_EXAMPLE_FOLDER = "data/example/"
NAME_EXAMPLE_FILE = "example_games"
PATH_DATA_RAW = "data/raw/"
LICHESS_PGN_FILE_NAME_PREFIX = "lichess_db_standard_rated"
INPUT_FILE_EXTENSION = ".pgn.zst"
LICHESS_DUMP_URL = "https://database.lichess.org/standard"
PATH_QUERY_FOLDER = "src/chessforge/queries"


# This is used for parsing the PGN files.
# Keys need to match the PGN headers
# INT columns will be parsed as integers
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
# NOTE ideas for features:
# elo diff
# number of moves
# board after X turns
# material after X turns