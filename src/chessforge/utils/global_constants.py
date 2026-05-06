PATH_EXAMPLE_FOLDER = "data/example/"
PATH_DATA_RAW = "data/raw/"
PATH_QUERY_FOLDER = "src/chessforge/queries"

URL_LICHESS_DUMP = "https://database.lichess.org/standard"

NAME_EXAMPLE_FILE = "example_games"

PREFIX_LICHESS_PGN_FILE = "lichess_db_standard_rated_" # as on https://database.lichess.org/standard
PREFIX_DOWNLOAD_TMP_FILE = "downloading_"

EXTENSION_INPUT_FILE = ".pgn.zst"
EXTENSION_DOWNLOAD_TMP_FILE = ".tmp"

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