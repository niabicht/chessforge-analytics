from io import StringIO
from typing import Any

import chess.pgn

from chessforge.utils.global_constants import GAME_COLUMNS  
from chessforge.utils.utils import int_or_none


def parse_game_string_into_dict(pgn_text: str) -> dict[str, Any]:
    """
    Parses a single pgn game string:
    Column names and types are defined by GAME_COLUMNS
    Casts types (e.g., String to Int) to match the database schema.

    Returns:
        dict: with columns as keys.
    """

    game = chess.pgn.read_game(StringIO(pgn_text))
    if game is None:
        raise ValueError(f"Could not parse PGN: {pgn_text[:100]}")

    headers = game.headers

    game_dict = {}
    for column, column_type in GAME_COLUMNS.items():
        value = headers.get(column)
        if column_type == "INT": value = int_or_none(value) # None if e.g. player has no elo
        game_dict[column] = value

    return game_dict