from dataclasses import dataclass
from typing import Callable

from chessforge.utils.utils import identity, int_or_none, str_or_none

@dataclass
class FeatureSpec:
    pgn_key: str           # Key in PGN headers, e.g. "WhiteElo"
    db_column: str         # Snake_case DB column name, e.e. "white_elo"
    db_type: str           # PostgreSQL type, e.g. "INT"
    encode: Callable       # Raw PGN string -> value to store in DB
    decode: Callable       # Stored value -> human-readable string



##############
### Strategies
##############

def _create_map_handlers(mapping: dict):
    """Returns (encode_func, decode_func) from a dictionary."""
    decode_map = {v: k for k, v in mapping.items()}
    
    def encode(val):
        return mapping.get(val) # Returns None if key (like "-") isn't found
        
    def decode(val):
        return decode_map.get(val, str(val))
        
    return encode, decode


### Feature: Result
_encode_result, _decode_result = _create_map_handlers({
    "0-1":     0, # Black wins
    "1/2-1/2": 1, # Draw
    "1-0":     2, # White wins
})


### Feature: ECO
def _encode_eco(eco_str: str | None) -> int | None:
    if not eco_str or len(eco_str) != 3: return None
    try:
        letter_map = {'A': 0, 'B': 1, 'C': 2, 'D': 3, 'E': 4}
        letter_val = letter_map.get(eco_str[0].upper())
        number_val = int(eco_str[1:3])
        if letter_val is None: return None
        return (letter_val * 100) + number_val
    except (ValueError, IndexError):
        return None

def _decode_eco(eco_int: int | None) -> str | None:
    if eco_int is None or not (0 <= eco_int <= 499): return None
    letters = "ABCDE"
    letter = letters[eco_int // 100]
    number = str(eco_int % 100).zfill(2)
    return f"{letter}{number}"


### Feature: TimeControl
def _encode_time_control(pgn_str: str) -> int | None:
    if not pgn_str or pgn_str == "-":
        return None
    try:
        # Handles "600+5" or "180" (no increment)
        parts = pgn_str.split("+")
        base = int(parts[0])
        increment = int(parts[1]) if len(parts) > 1 else 0
        return base + (increment * 40)
    except (ValueError, IndexError):
        return None

def _decode_time_control(total_seconds: int | None) -> str:
    if total_seconds is None:
        return "Unknown"
    
    if total_seconds < 30:
        return "UltraBullet"
    if total_seconds < 180:
        return "Bullet"
    if total_seconds < 600:
        return "Blitz"
    if total_seconds < 1800:
        return "Rapid"
    return "Classical"


# NOTE ideas for features:
# elo diff
# number of moves
# board after X turns
# material after X turns
# engine rating when available (6% of games apparently)



####################
### Feature Registry
####################

FEATURES: list[FeatureSpec] = [
    FeatureSpec(
        pgn_key="Result",
        db_column="result",
        db_type="SMALLINT",
        encode=_encode_result,
        decode=_decode_result,
    ),
    FeatureSpec(
        pgn_key="WhiteElo",
        db_column="white_elo",
        db_type="INT",
        encode=int_or_none,
        decode=str,
    ),
    FeatureSpec(
        pgn_key="BlackElo",
        db_column="black_elo",
        db_type="INT",
        encode=int_or_none,
        decode=str,
    ),
    FeatureSpec(
        pgn_key="ECO",
        db_column="eco",
        db_type="SMALLINT",
        encode=_encode_eco,
        decode=_decode_eco,
    ),
    FeatureSpec(
        pgn_key="Opening",
        db_column="opening",
        db_type="TEXT",
        encode=str_or_none,
        decode=identity,
    ),
    FeatureSpec(
        pgn_key="TimeControl",
        db_column="time_control",
        db_type="TEXT",
        encode=_encode_time_control,
        decode=_decode_time_control,
    ),
]

# Derived lookup by pgn_key — use this in parser.py
FEATURES_BY_PGN_KEY: dict[str, FeatureSpec] = {
    spec.pgn_key: spec for spec in FEATURES
}

# Backward-compatible GAME_COLUMNS shape: {pgn_key: db_type}
# Used by schema.py and repository.py, no changes needed there yet
GAME_COLUMNS: dict[str, str] = {
    spec.pgn_key: spec.db_type for spec in FEATURES
}
