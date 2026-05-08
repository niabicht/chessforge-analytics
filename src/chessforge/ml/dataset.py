import torch
from torch.utils.data import Dataset


class ChessDataset(Dataset):
    def __init__(self, games: list[dict], preprocessor):
        self.data = [preprocessor.transform(g) for g in games]

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        item = self.data[idx]
        return {
            "features": torch.tensor(item["features"]), # TODO we should probably define or construct this soemwhere centrally
            "eco": torch.tensor(item["eco"]),
            "label": torch.tensor(item["label"])
        }