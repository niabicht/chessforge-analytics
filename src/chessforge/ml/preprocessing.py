import joblib
import numpy as np
from sklearn.preprocessing import StandardScaler
from chessforge.ingestion.feature_registry import encode_eco


class ChessPreprocessor:
    def __init__(self):
        self.elo_scaler = StandardScaler()
        self.is_fitted = False

    def fit(self, games_list: list[dict]):
        # Extract Elos for standard scaling
        elos = np.array([[g['white_elo'], g['black_elo']] for g in games_list if g['white_elo'] and g['black_elo']])
        self.elo_scaler.fit(elos)
        self.is_fitted = True

    def transform(self, game: dict):
        """Transforms a single raw game dict into ML-ready numerical values."""
        # 1. Elo Standard Scaling
        elo_scaled = self.elo_scaler.transform([[game['white_elo'], game['black_elo']]])[0]
        
        # 2. Time Control: Log Scaling
        # Default to 0 if None, +1 to avoid log(0)
        time_val = game['time_control'] if game['time_control'] is not None else 0
        time_log = np.log1p(time_val)

        # 3. ECO: Already an int (0-499) from your ingestion
        eco_int = game['eco'] if game['eco'] is not None else 0
        
        return {
            "features": np.array([elo_scaled[0], elo_scaled[1], time_log], dtype=np.float32),
            "eco": int(eco_int),
            "label": int(game['result']) if game['result'] is not None else None
        }

    def save(self, path: str):
        joblib.dump(self.elo_scaler, path)

    def load(self, path: str):
        self.elo_scaler = joblib.load(path)
        self.is_fitted = True