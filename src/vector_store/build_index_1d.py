import shutil
from datetime import datetime
from pathlib import Path
import numpy as np

from features.feature_builder_1d import build_feature_vectors
from vector_store.faiss_index import MarketStateFAISS


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

VECTOR_DIM = 8

FAISS_ROOT = Path("shared_data/faiss")
BASE_NAME = "market_state_1d"

LATEST_PATH = FAISS_ROOT / BASE_NAME


# ─────────────────────────────────────────────
# BUILD WITH VERSIONING
# ─────────────────────────────────────────────

def build_faiss_index():
    print("🚀 Building FAISS index (with versioning)...")

    data = build_feature_vectors()

    if not data:
        raise RuntimeError("❌ No feature vectors returned")

    vectors = np.array([v for v, _ in data], dtype="float32")
    metadata = [m for _, m in data]

    print(f"📦 Total vectors: {vectors.shape[0]}")

    # 1️⃣ Create versioned folder
    ts = datetime.now().strftime("%Y_%m_%d_%H%M")
    versioned_path = FAISS_ROOT / f"{BASE_NAME}_v{ts}"
    versioned_path.mkdir(parents=True, exist_ok=True)

    # 2️⃣ Build FAISS
    store = MarketStateFAISS(dim=VECTOR_DIM)
    store.add(vectors, metadata)
    store.save(versioned_path)

    print(f"✅ Versioned FAISS index written to: {versioned_path}")

    # 3️⃣ Update "latest" pointer (atomic replace)
    if LATEST_PATH.exists():
        shutil.rmtree(LATEST_PATH)

    shutil.copytree(versioned_path, LATEST_PATH)

    print(f"🔁 Updated latest FAISS index → {LATEST_PATH}")


if __name__ == "__main__":
    build_faiss_index()
