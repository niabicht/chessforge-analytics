"""
model.py
Responsible for:
  - Neural network architecture (driven by feature_registry metadata)
  - Training loop with loss + optimizer
  - Saving/loading as ONNX
  - Running inference from numpy arrays

No knowledge of specific feature names, shape is derived from feature_registry.
"""

import os

import numpy as np
import onnxruntime as onnx
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

from chessforge.ingestion.feature_registry import EMBEDDING_FEATURES, NUMERIC_FEATURES
from chessforge.utils.global_constants import PATH_MODEL_DIR

_ONNX_PATH = os.path.join(PATH_MODEL_DIR, "model.onnx")

# Model hyperparameters (hardcoded for now)
_NUM_CLASSES = 3   # black win / draw / white win
_HIDDEN_DIMENSIONS = [256, 128, 64]
_DROPOUT_RATE = 0.2
_N_EPOCHS = 20


class ChessOutcomeNN(nn.Module):
    """
    Architecture:
      - One nn.Embedding per embedding feature (sizes from feature_registry)
      - All embeddings concatenated with numeric features
      - Three hidden layers, each: Linear -> BatchNorm -> ReLU -> Dropout
      - Output: Linear -> 3 logits (softmax applied at inference time)
    """

    def __init__(self):
        super().__init__()

        # Build one embedding per embedding feature
        self.embeddings = nn.ModuleList([
            nn.Embedding(spec.ml.n_embeddings, spec.ml.embedding_dimension)
            for spec in EMBEDDING_FEATURES
        ])

        # Input dimension = numeric features + flattened embedding outputs
        n_numeric = len(NUMERIC_FEATURES)
        n_emb_total = sum(spec.ml.embedding_dimension for spec in EMBEDDING_FEATURES)
        input_dim = n_numeric + n_emb_total

        # Hidden layers
        layers = []
        previous_dimension = input_dim
        for i, hidden_dim in enumerate(_HIDDEN_DIMENSIONS):
            layers += [
                nn.Linear(previous_dimension, hidden_dim),
                nn.BatchNorm1d(hidden_dim),
                nn.LeakyReLU(0.05),
            ]
            if i < len(_HIDDEN_DIMENSIONS) - 1: layers.append(nn.Dropout(_DROPOUT_RATE)) # No dropout in last hidden layer to allow stable representation before the output
            previous_dimension = hidden_dim

        self.hidden = nn.Sequential(*layers)
        self.output = nn.Linear(previous_dimension, _NUM_CLASSES)

    def forward(self, x_numeric: torch.Tensor, x_emb: torch.Tensor) -> torch.Tensor:
        # x_numeric: [batch, n_numeric]  float32
        # x_emb:     [batch, n_emb_features]  int64

        emb_parts = [
            emb_layer(x_emb[:, i])
            for i, emb_layer in enumerate(self.embeddings)
        ]
        x = torch.cat([x_numeric] + emb_parts, dim=1)
        x = self.hidden(x)
        return self.output(x)  # logits, not softmax



def train_and_save_model(
    X_num_train: np.ndarray, X_emb_train: np.ndarray, y_train: np.ndarray,
    X_num_val:   np.ndarray, X_emb_val:   np.ndarray, y_val:   np.ndarray,
    batch_size: int = 2048,
    lr: float = 1e-3,
    log=lambda message: None,
) -> None:
    """
    Train the model, print epoch-level metrics, then export to ONNX.
    Overwrites any existing model.
    """
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu") # TODO test
    log(f"Training on {device}  |  train={len(y_train):,}  val={len(y_val):,}")

    # set seed TODO test or do I also need random.seed(seed) and np.random.seed(seed)?
    seed = 42
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed(seed)
        torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False

    # Tensors
    def to_tensors(xn, xe, y):
        return (
            torch.tensor(xn, dtype=torch.float32),
            torch.tensor(xe, dtype=torch.int64),
            torch.tensor(y,  dtype=torch.long),
        )

    train_ds = TensorDataset(*to_tensors(X_num_train, X_emb_train, y_train))
    val_ds   = TensorDataset(*to_tensors(X_num_val,   X_emb_val,   y_val))

    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    val_loader   = DataLoader(val_ds,   batch_size=batch_size)

    model = ChessOutcomeNN().to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    criterion = nn.CrossEntropyLoss()

    for epoch in range(1, _N_EPOCHS + 1):
        # train
        model.train()
        total_loss, correct, total = 0.0, 0, 0
        for xn, xe, y in train_loader:
            xn, xe, y = xn.to(device), xe.to(device), y.to(device)
            optimizer.zero_grad()
            logits = model(xn, xe)
            loss = criterion(logits, y)
            loss.backward()
            optimizer.step()
            total_loss += loss.item() * len(y)
            correct += (logits.argmax(1) == y).sum().item()
            total += len(y)

        train_acc = correct / total
        train_loss = total_loss / total

        # validate
        model.eval()
        val_loss, val_correct, val_total = 0.0, 0, 0
        with torch.no_grad():
            for xn, xe, y in val_loader:
                xn, xe, y = xn.to(device), xe.to(device), y.to(device)
                logits = model(xn, xe)
                val_loss += criterion(logits, y).item() * len(y)
                val_correct += (logits.argmax(1) == y).sum().item()
                val_total += len(y)

        log(
            f"Epoch {epoch:3}/{_N_EPOCHS}  "
            f"train loss={train_loss:.4f} acc={train_acc:.3f}  "
            f"val loss={val_loss/val_total:.4f} acc={val_correct/val_total:.3f}"
        )

    _export_onnx(model, device, log)


def _export_onnx(model: ChessOutcomeNN, device: torch.device, log=lambda message: None,) -> None:
    """Export trained model to ONNX, overwriting any previous file."""
    os.makedirs(PATH_MODEL_DIR, exist_ok=True)
    model.eval()

    n_numeric = len(NUMERIC_FEATURES)
    n_embedding_features = len(EMBEDDING_FEATURES)

    dummy_numeric_features = torch.zeros(1, n_numeric, dtype=torch.float32).to(device)
    dummy_embedding_features = torch.zeros(1, n_embedding_features, dtype=torch.int64).to(device)

    torch.onnx.export(
        model,
        (dummy_numeric_features, dummy_embedding_features),
        _ONNX_PATH,
        input_names=["numeric", "embeddings"],
        output_names=["logits"],
        dynamic_axes={
            "numeric":    {0: "batch"},
            "embeddings": {0: "batch"},
            "logits":     {0: "batch"},
        },
        opset_version=17,
    )
    log(f"Model saved to {_ONNX_PATH}")


def predict_probabilities(
    numeric: np.ndarray, # [1, n_numeric]  float32
    embeddings: np.ndarray, # [1, n_emb_features]  int64
) -> np.ndarray:
    """Load ONNX model and return softmax probabilities [black_win, draw, white_win]."""
    if not os.path.exists(_ONNX_PATH):
        raise FileNotFoundError(f"No ONNX model found at {_ONNX_PATH}. Run train-nn first.")

    session = onnx.InferenceSession(_ONNX_PATH, providers=["CPUExecutionProvider"]) # NOTE If we were later to do bulks of predictions, one session per prediction would be very inefficient
    logits = session.run(
        ["logits"],
        {"numeric": numeric, "embeddings": embeddings},
    )[0]  # [1, 3]

    exp = np.exp(logits - logits.max(axis=1, keepdims=True))
    probabilities = exp / exp.sum(axis=1, keepdims=True)
    return probabilities[0] # [3]
    # probabilities = torch.softmax(torch.from_numpy(logits), dim=1) # TODO test if this works the same
    # return probabilities[0].numpy() # [3]
