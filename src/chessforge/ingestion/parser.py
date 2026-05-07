from io import StringIO
from typing import Any

import chess.pgn

from chessforge.ingestion.feature_registry import FEATURES_BY_PGN_KEY


def parse_game_string_into_dict(pgn_text: str) -> dict[str, Any]:
    """
    Parses a single pgn game string:
    Column names and types are defined in feature_registry.py
    Encoding like certain strings to ints are also defined there.

    Returns:
        dict: with columns as keys.
    """

    game = chess.pgn.read_game(StringIO(pgn_text))
    if game is None:
        raise ValueError(f"Could not parse PGN: {pgn_text[:100]}")

    headers = game.headers

    game_dict = {}    
    for pgn_key, spec in FEATURES_BY_PGN_KEY.items():
        game_dict[pgn_key] = spec.encode(headers.get(pgn_key))

    return game_dict