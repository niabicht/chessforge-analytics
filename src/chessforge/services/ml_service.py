import os

import torch # TODO can I maybe use a cpu-only package to avoid all of that cuda stuff I may not need
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, random_split

from chessforge.database.connections import InitializedConnection
from chessforge.database.repository import execute_query_return_result
from chessforge.ingestion.feature_registry import encode_eco, encode_time_control, FEATURES_BY_PGN_KEY
from chessforge.ml.preprocessing import ChessPreprocessor
from chessforge.ml.model import NNPredictOutcome
from chessforge.ml.dataset import ChessDataset


# Paths for artifacts
MODEL_DIR = "data/models"
SCALER_PATH = os.path.join(MODEL_DIR, "scaler.joblib")
MODEL_PATH = os.path.join(MODEL_DIR, "model.pth")
ONNX_PATH = os.path.join(MODEL_DIR, "model.onnx")


class MLService:
    def __init__(self):
        self.preprocessor = ChessPreprocessor()
        self.model = None
        os.makedirs(MODEL_DIR, exist_ok=True)

    def prepare_nn(self, log=print):
        """Fetches all data to fit the scalers."""
        log("Fetching games for preprocessing...")
        with InitializedConnection() as connection:
            # We fetch everything just to get the distribution of Elos and Time
            games = execute_query_return_result(connection, "SELECT white_elo, black_elo, time_control FROM games")
        
        if not games:
            log("No data found in DB. Ingest some games first.")
            return False

        self.preprocessor.fit(games)
        self.preprocessor.save(SCALER_PATH)
        log(f"Scalers fitted and saved to {SCALER_PATH}")
        return True


    def train_nn(self, epochs=10, batch_size=64, log=print):
        """Orchestrates the training loop and validation."""
        # 1. Load Preprocessor
        if os.path.exists(SCALER_PATH):
            self.preprocessor.load(SCALER_PATH)
        else:
            log("Scaler not found. Run prepare-nn first.")
            return False

        # 2. Prepare Data
        log("Loading datasets from DB...")
        with InitializedConnection() as connection:
            raw_rows = execute_query_return_result(connection, 
                "SELECT white_elo, black_elo, time_control, eco, result FROM games")

        dataset = ChessDataset(raw_rows, self.preprocessor)
        train_size = int(0.8 * len(dataset))
        val_size = len(dataset) - train_size
        train_set, val_set = random_split(dataset, [train_size, val_size])

        train_loader = DataLoader(train_set, batch_size=batch_size, shuffle=True)
        val_loader = DataLoader(val_set, batch_size=batch_size)

        # 3. Initialize Model
        self.model = NNPredictOutcome()
        criterion = nn.CrossEntropyLoss()
        optimizer = optim.Adam(self.model.parameters(), lr=0.001)

        # 4. Training Loop
        log(f"Starting training on {train_size} games...")
        for epoch in range(epochs):
            self.model.train()
            total_loss = 0
            for batch in train_loader:
                optimizer.zero_grad()
                outputs = self.model(batch["features"], batch["eco"])
                loss = criterion(outputs, batch["label"])
                loss.backward()
                optimizer.step()
                total_loss += loss.item()
            
            # Validation
            val_acc = self._evaluate(val_loader)
            log(f"Epoch {epoch+1}/{epochs} - Loss: {total_loss/len(train_loader):.4f} - Val Acc: {val_acc:.2f}%")

        # 5. Save Artifacts
        torch.save(self.model.state_dict(), MODEL_PATH)
        self._export_onnx(log)
        return True


    def _evaluate(self, loader):
        self.model.eval()
        correct = 0
        total = 0
        with torch.no_grad():
            for batch in loader:
                outputs = self.model(batch["features"], batch["eco"])
                _, predicted = torch.max(outputs.data, 1)
                total += batch["label"].size(0)
                correct += (predicted == batch["label"]).sum().item()
        return 100 * correct / total
    

    def predict(self, white_elo: int, black_elo: int, eco_str: str, time_control_str: str) -> str:
        # 1. Load artifacts if not in memory
        if not self.model:
            if not os.path.exists(MODEL_PATH) or not os.path.exists(SCALER_PATH):
                return "Error: Model or Scaler not found. Train the model first."

            self.preprocessor.load(SCALER_PATH)
            self.model = NNPredictOutcome()
            self.model.load_state_dict(torch.load(MODEL_PATH))
            self.model.eval()

        # 2. Transform raw inputs using our registry logic
        eco_int = encode_eco(eco_str) or 0
        time_seconds = encode_time_control(time_control_str) or 0

        # Create a dummy dict to reuse the preprocessor logic
        game_dict = {
            "white_elo": white_elo,
            "black_elo": black_elo,
            "time_control": time_seconds,
            "eco": eco_int,
            "result": None # Result does not matter for prediction
        }

        processed = self.preprocessor.transform(game_dict)

        # 3. Inference
        with torch.no_grad():
            features_tensor = torch.tensor(processed["features"]).unsqueeze(0) # Add batch dimension
            eco_tensor = torch.tensor([processed["eco"]]).long()

            outputs = self.model(features_tensor, eco_tensor)
            _, predicted = torch.max(outputs, 1)
            prediction_idx = predicted.item()

        # 4. Map back to human readable result via the registry
        # 0: Black wins, 1: Draw, 2: White wins
        result_map = FEATURES_BY_PGN_KEY["Result"].decode
        return result_map(prediction_idx)


    def _export_onnx(self, log):
        """Exports the model to ONNX format for later use."""
        self.model.eval()
        dummy_features = torch.randn(1, 3)
        dummy_eco = torch.randint(0, 500, (1,))
        torch.onnx.export(self.model, (dummy_features, dummy_eco), ONNX_PATH,
                          input_names=['features', 'eco'],
                          output_names=['outcome'],
                          dynamic_axes={'features': {0: 'batch_size'}, 'eco': {0: 'batch_size'}})
        log(f"Model exported to {ONNX_PATH}")