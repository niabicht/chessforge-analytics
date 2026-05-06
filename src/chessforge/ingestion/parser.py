from io import StringIO
from typing import Any

import chess.pgn

from chessforge.utils.global_constants import GAME_COLUMNS  
from chessforge.utils.utils import int_or_none


def parse_game_string_into_dict(pgn_text: str) -> dict[str, Any]:
    """
    TODO (dict has colum as key and value of whatever type)
    """

    game = chess.pgn.read_game(StringIO(pgn_text))
    if game is None:
        print(f"WARNING: could not parse PGN: {pgn_text[:100]}") # Should not happen # TODO refactor according to log rules
        return None

    headers = game.headers

    game_dict = {}
    for column, column_type in GAME_COLUMNS.items():
        value = headers.get(column)
        if column_type == "INT": value = int_or_none(value) # None if e.g. player has no elo
        game_dict[column] = value

    return game_dict