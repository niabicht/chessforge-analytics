"""
preprocessor.py
Responsible for:
  - Dropping rows with missing ML-relevant values
  - Applying log1p transforms where specified in MLSpec
  - Fitting StandardScalers for numeric features and saving them (joblib)
  - Transforming raw DB rows into (X_numeric, X_embedding, y) tensors
  - Train/test splitting
  - Loading scalers for inference

No knowledge of specific feature names, all logic is driven by feature_registry.
"""

import os

import joblib
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from chessforge.ingestion.feature_registry import TARGET_FEATURE, NUMERIC_FEATURES, EMBEDDING_FEATURES
from chessforge.utils.global_constants import PATH_MODEL_DIR


_SCALER_PATH = os.path.join(PATH_MODEL_DIR, "scaler.joblib")


###########
### Helpers
###########

def _get_numeric_columns() -> list[str]:
    return [feature.db_column for feature in NUMERIC_FEATURES]


def _get_embedding_columns() -> list[str]:
    return [feature.db_column for feature in EMBEDDING_FEATURES]


def _target_col() -> str:
    return TARGET_FEATURE.db_column


def _rows_to_arrays(rows: list[dict]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Convert DB rows to raw numpy arrays (no scaling yet)."""
    numeric  = np.array([[row[column] for column in _get_numeric_columns()] for row in rows], dtype=np.float32)
    embeddings = np.array([[row[column] for column in _get_embedding_columns()] for row in rows], dtype=np.int64)
    targets  = np.array([row[_target_col()] for row in rows], dtype=np.int64)
    return numeric, embeddings, targets


def _apply_log1p_conditionally(numeric_features: np.ndarray) -> np.ndarray:
    """Apply log1p to columns that request it (in-place on a copy)."""
    result = numeric_features.copy()
    for i, spec in enumerate(NUMERIC_FEATURES):
        if spec.ml.log1p:
            result[:, i] = np.log1p(result[:, i])
    return result



##############
### Public API
##############

def drop_incomplete_rows(rows: list[dict]) -> list[dict]:
    """Remove any row that has None in a column needed for ML."""
    required_columns = _get_numeric_columns() + _get_embedding_columns() + [_target_col()]
    return [row for row in rows if all(row.get(column) is not None for column in required_columns)]


def fit_and_save_scalers(rows: list[dict]) -> None:
    """
    Fit one StandardScaler on all numeric features (post log1p) and save it.
    Called before training.
    """
    numeric, _, _ = _rows_to_arrays(rows)
    numeric = _apply_log1p_conditionally(numeric)

    scaler = StandardScaler()
    scaler.fit(numeric)

    os.makedirs(PATH_MODEL_DIR, exist_ok=True)
    joblib.dump(scaler, _SCALER_PATH)


def load_scalers() -> StandardScaler:
    if not os.path.exists(_SCALER_PATH):
        raise FileNotFoundError(f"No scaler found at {_SCALER_PATH}. Run train-nn first.")
    return joblib.load(_SCALER_PATH)


def prepare_training_data(
    rows: list[dict],
    test_size: float = 0.1,
) -> tuple:
    """
    Full preprocessing pipeline for training.

    Returns:
        (X_num_train, X_emb_train, y_train,
         X_num_val,   X_emb_val,   y_val)
    All as numpy arrays; scaling is applied.
    """
    rows = drop_incomplete_rows(rows) # Should already be done in service but just to be sure
    fit_and_save_scalers(rows)
    scaler = load_scalers()

    numeric, embeddings, targets = _rows_to_arrays(rows)
    numeric = _apply_log1p_conditionally(numeric)
    numeric = scaler.transform(numeric).astype(np.float32)

    # Split indices
    idx = np.arange(len(targets))
    idx_train, idx_val = train_test_split(idx, test_size=test_size, random_state=42, stratify=targets)

    return (
        numeric[idx_train], embeddings[idx_train], targets[idx_train],
        numeric[idx_val],   embeddings[idx_val],   targets[idx_val],
    )


def transform_single_input(
    feature_values: dict,
    scaler: StandardScaler,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Preprocess one inference sample.

    Args:
        feature_values: dict keyed by db_column, e.g.
            {"white_elo": 1500, "black_elo": 1600, "eco": 114, "time_control": 300}
        scaler: loaded StandardScaler

    Returns:
        (numeric_array [1, n_numeric], embedding_array [1, n_embedding])
    """
    numeric_features = np.array(
        [[feature_values[column] for column in _get_numeric_columns()]], dtype=np.float32
    )
    numeric_features = _apply_log1p_conditionally(numeric_features)
    numeric_features = scaler.transform(numeric_features).astype(np.float32)

    embeddings = np.array(
        [[feature_values[column] for column in _get_embedding_columns()]], dtype=np.int64
    )
    return numeric_features, embeddings
