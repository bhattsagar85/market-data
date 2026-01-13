import faiss
import numpy as np
import pickle
from pathlib import Path
from typing import List, Dict


class MarketStateFAISS:
    """
    Thin wrapper around FAISS index + metadata.
    Read-optimized, append-only.
    """

    def __init__(self, dim: int):
        self.dim = dim

        # HNSW = fast, memory-efficient, great for similarity search
        self.index = faiss.IndexHNSWFlat(dim, 32)
        self.index.hnsw.efConstruction = 200
        self.index.hnsw.efSearch = 50

        self.metadata: List[Dict] = []

    # ─────────────────────────────────────────────
    # Write
    # ─────────────────────────────────────────────

    def add(self, vectors: np.ndarray, metadata: List[Dict]):
        assert vectors.shape[1] == self.dim
        assert len(vectors) == len(metadata)

        self.index.add(vectors.astype("float32"))
        self.metadata.extend(metadata)

    def save(self, path: Path):
        path.mkdir(parents=True, exist_ok=True)

        faiss.write_index(self.index, str(path / "index.faiss"))

        with open(path / "metadata.pkl", "wb") as f:
            pickle.dump(self.metadata, f)

    # ─────────────────────────────────────────────
    # Read
    # ─────────────────────────────────────────────

    @classmethod
    def load(cls, path: Path, dim: int):
        obj = cls(dim)

        obj.index = faiss.read_index(str(path / "index.faiss"))

        with open(path / "metadata.pkl", "rb") as f:
            obj.metadata = pickle.load(f)

        return obj

    def search(self, query: np.ndarray, k: int = 10):
        """
        query: shape (dim,)
        returns: list of {distance, metadata}
        """
        query = query.reshape(1, -1).astype("float32")

        distances, indices = self.index.search(query, k)

        results = []
        for idx, dist in zip(indices[0], distances[0]):
            results.append(
                {
                    "distance": float(dist),
                    "metadata": self.metadata[idx],
                }
            )

        return results
