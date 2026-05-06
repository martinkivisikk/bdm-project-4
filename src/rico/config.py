import os

# ── PostgreSQL ─────────────────────────────────────────────────────────────
# Build a libpq-style DSN from individual env vars so each can be overridden
# independently (useful in tests). Inside Docker the host is "postgres";
# outside (local dev / lab) it defaults to localhost.
POSTGRES_DSN = (
    "host={host} port={port} dbname={dbname} user={user} password={password}".format(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "rico"),
        user=os.getenv("POSTGRES_USER", "rico"),
        password=os.getenv("POSTGRES_PASSWORD", "rico"),
    )
)

# ── MinIO / S3 ─────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",     "rico-raw")

# ── Ollama ─────────────────────────────────────────────────────────────────
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL",    "qwen2.5:3b")

# ── ML model versions (pinned for reproducibility) ─────────────────────────
CLIP_MODEL_NAME       = "ViT-B-32"
CLIP_MODEL_PRETRAINED = "laion2b_s34b_b79k"   # → 512-dim vectors
SBERT_MODEL_NAME      = "sentence-transformers/all-MiniLM-L6-v2"  # → 384-dim

PROMPT_VERSION = "v1"
