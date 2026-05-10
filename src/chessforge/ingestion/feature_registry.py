from dataclasses import dataclass, field
from typing import Callable, Literal

from chessforge.utils.utils import identity, int_or_none, str_or_none


### ML role types
# "target"    – the label column (result); excluded from model inputs
# "numeric"   – continuous value that gets scaled (optionally log-transformed first)
# "embedding" – integer index fed into an nn.Embedding layer
# None        – feature exists in DB but is not used for ML at all
MLRole = Literal["target", "numeric", "embedding"] | None


@dataclass
class MLSpec:
    """Everything the ML pipeline needs to know about one feature."""
    role: MLRole = None

    # IMPORTANT! numeric only
    log1p: bool = False         # Apply np.log1p before standard scaling
                                # Scaler is always StandardScaler for numeric features

    # IMPORTANT! embedding only
    n_embeddings: int = 0     # Vocabulary size (e.g. 500 for ECO)
    embedding_dimension: int = 0      # Output dimension (e.g. 8)



@dataclass
class FeatureSpec:
    pgn_key: str           # Key in PGN headers, e.g. "WhiteElo"
    db_column: str         # Snake_case DB column name, e.e. "white_elo"
    db_type: str           # PostgreSQL type, e.g. "INT"
    encode: Callable       # Raw PGN string -> value to store in DB
    decode: Callable       # Stored value -> human-readable string
    ml: MLSpec = field(default_factory=MLSpec)


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
encode_result, decode_result = _create_map_handlers({
    "0-1":     0, # Black wins
    "1/2-1/2": 1, # Draw
    "1-0":     2, # White wins
})


### Feature: ECO
def encode_eco(eco_str: str | None) -> int | None:
    if not eco_str or len(eco_str) != 3: return None
    try:
        letter_map = {'A': 0, 'B': 1, 'C': 2, 'D': 3, 'E': 4}
        letter_val = letter_map.get(eco_str[0].upper())
        number_val = int(eco_str[1:3])
        if letter_val is None: return None
        return (letter_val * 100) + number_val
    except (ValueError, IndexError):
        return None


def decode_eco(eco_int: int | None) -> str | None:
    if eco_int is None or not (0 <= eco_int <= 499): return None
    letters = "ABCDE"
    letter = letters[eco_int // 100]
    number = str(eco_int % 100).zfill(2)
    return f"{letter}{number}"


### Feature: TimeControl
def encode_time_control(pgn_str: str) -> int | None:
    if not pgn_str or pgn_str == "-": return None
    try:
        # Handles "600+5" or "180" (no increment)
        parts = pgn_str.split("+")
        base = int(parts[0])
        increment = int(parts[1]) if len(parts) > 1 else 0
        return base + (increment * 40)
    except (ValueError, IndexError):
        return None


def decode_time_control(total_seconds: int | None) -> str:
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


# NOTE ideas for features to maybe add later:
# elo diff
# number of moves
# board after X turns (with CNNs)
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
        encode=encode_result,
        decode=decode_result,
        ml=MLSpec(role="target"),
    ),
    FeatureSpec(
        pgn_key="WhiteElo",
        db_column="white_elo",
        db_type="SMALLINT",
        encode=int_or_none,
        decode=str,
        ml=MLSpec(role="numeric"),
    ),
    FeatureSpec(
        pgn_key="BlackElo",
        db_column="black_elo",
        db_type="SMALLINT",
        encode=int_or_none,
        decode=str,
        ml=MLSpec(role="numeric"),
    ),
    FeatureSpec(
        pgn_key="ECO",
        db_column="eco",
        db_type="SMALLINT",
        encode=encode_eco,
        decode=decode_eco,
        ml=MLSpec(role="embedding", n_embeddings=500, embedding_dimension=8),
    ),
    FeatureSpec(
        pgn_key="Opening",
        db_column="opening",
        db_type="TEXT",
        encode=str_or_none,
        decode=identity,
        ml=MLSpec(role=None), # Not used in ML
    ),
    FeatureSpec(
        pgn_key="TimeControl",
        db_column="time_control",
        db_type="SMALLINT",
        encode=encode_time_control,
        decode=decode_time_control,
        ml=MLSpec(role="numeric", log1p=True),
    ),
]



###################
### Derived Lookups
###################

# Derived lookup by pgn_key. Used in parser.py
FEATURES_BY_PGN_KEY: dict[str, FeatureSpec] = {
    spec.pgn_key: spec for spec in FEATURES
}

# Backward-compatible GAME_COLUMNS shape: {pgn_key: db_type}
# Used by schema.py and repository.py, no changes needed there yet
GAME_COLUMNS: dict[str, str] = {
    spec.pgn_key: spec.db_type for spec in FEATURES
}


# ML-only derived views
TARGET_FEATURE: FeatureSpec = next(feature for feature in FEATURES if feature.ml.role == "target")
NUMERIC_FEATURES: list[FeatureSpec] = [feature for feature in FEATURES if feature.ml.role == "numeric"]
EMBEDDING_FEATURES: list[FeatureSpec] = [feature for feature in FEATURES if feature.ml.role == "embedding"]