# src/chessforge/ml/model.py
import torch
import torch.nn as nn


class NNPredictOutcome(nn.Module):
    def __init__(self, embedding_dim=16, hidden_size=64):
        super(NNPredictOutcome, self).__init__()
        
        # ECO Embedding: 500 possible codes -> embedding_dim vector
        self.eco_embedding = nn.Embedding(500, embedding_dim)
        
        # Input size: 3 (WhiteElo, BlackElo, Time) + embedding_dim
        input_size = 3 + embedding_dim
        
        self.layers = nn.Sequential(
            # Layer 1
            nn.Linear(input_size, hidden_size),
            nn.BatchNorm1d(hidden_size),
            nn.LeakyReLU(0.01),
            nn.Dropout(0.2),
            
            # Layer 2
            nn.Linear(hidden_size, hidden_size),
            nn.BatchNorm1d(hidden_size),
            nn.LeakyReLU(0.01),
            nn.Dropout(0.2),
            
            # Layer 3
            nn.Linear(hidden_size, hidden_size // 2),
            nn.BatchNorm1d(hidden_size // 2),
            nn.LeakyReLU(0.01),
            
            # Output: 3 classes (0: Black, 1: Draw, 2: White)
            nn.Linear(hidden_size // 2, 3)
        )

    def forward(self, features, eco):
        eco_emb = self.eco_embedding(eco)
        # Concatenate numerical features with ECO embedding
        x = torch.cat([features, eco_emb], dim=1)
        return self.layers(x)
    

    