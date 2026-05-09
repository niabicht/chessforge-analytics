"""
ml_service.py
Responsible for:
  - Fetching raw data from the database
  - Delegating preprocessing to preprocessor.py
  - Delegating training / inference to model.py
  - Translating CLI inputs (e.g. "B14", "180+2") into DB-encoded values via feature_registry
  - Logging results back to the CLI
"""

import chessforge.database.connections as connections
import chessforge.database.repository as repository
from chessforge.ingestion.feature_registry import FEATURES_BY_PGN_KEY, NUMERIC_FEATURES, EMBEDDING_FEATURES, TARGET_FEATURE
from chessforge.ml import model as nn_model
from chessforge.ml import preprocessor



def _load_rows(log=lambda message: None) -> list[dict] | None:
    """Fetch all ML-relevant columns from the database."""
    needed_cols = (
        [TARGET_FEATURE.db_column]
        + [s.db_column for s in NUMERIC_FEATURES]
        + [s.db_column for s in EMBEDDING_FEATURES]
    )
    col_list = ", ".join(needed_cols)
    sql = f"SELECT {col_list} FROM games;"
    with connections.InitializedConnection() as connection:
        rows = repository.execute_query_return_result(connection, sql)
    rows = [dict(r) for r in rows]
    log(f"Loaded {len(rows):,} rows from database.")
    return rows


def train_nn(log=lambda message: None) -> bool:
    rows = _load_rows(log)
    if not rows:
        log("No data found. Ingest some games first.")
        return False
    
    rows = preprocessor.drop_incomplete_rows(rows)
    log(f"{len(rows):,} rows remaining after dropping incomplete entries.")
    if len(rows) < 100:
        log("Not enough clean rows to train. Aborting.")
        return False
    
    (
        X_num_train, X_emb_train, y_train,
        X_num_val,   X_emb_val,   y_val,
    ) = preprocessor.prepare_training_data(rows)

    nn_model.train_and_save_model(
        X_num_train, X_emb_train, y_train,
        X_num_val,   X_emb_val,   y_val,
        log=log,
    )

    return True


def predict(
    white_elo: int,
    black_elo: int,
    eco_str: str,
    time_control_str: str,
    log=lambda message: None,
) -> bool:
    # Encode CLI strings to DB-stored values using feature_registry encode functions
    eco_val = FEATURES_BY_PGN_KEY["ECO"].encode(eco_str)
    time_control_val  = FEATURES_BY_PGN_KEY["TimeControl"].encode(time_control_str)

    if eco_val is None:
        log(f"Could not parse ECO code '{eco_str}'. Expected format: A00 to E99.")
        return False
    
    if time_control_val is None:
        log(f"Could not parse time control '{time_control_str}'. Expected format: 180+2 or 300.")
        return False
    
    feature_values = {
        "white_elo":    white_elo,
        "black_elo":    black_elo,
        "eco":          eco_val,
        "time_control": time_control_val,
    }

    try:
        scaler = preprocessor.load_scalers()
    except FileNotFoundError as e:
        log(str(e))
        return False
    
    numeric, embeddings = preprocessor.transform_single_input(feature_values, scaler)
    try:
        probabilities = nn_model.predict_probabilities(numeric, embeddings)
    except FileNotFoundError as e:
        log(str(e))
        return False
        
    black_win, draw, white_win = probabilities
    log(
        f"\nPredicted outcome probabilities:\n"
        f"  White win : {white_win * 100:5.1f}%\n"
        f"  Draw      : {draw      * 100:5.1f}%\n"
        f"  Black win : {black_win * 100:5.1f}%"
    )
    return True


def _decode_feature(pgn_key: str, value) -> str:
    return FEATURES_BY_PGN_KEY[pgn_key].decode(value)
