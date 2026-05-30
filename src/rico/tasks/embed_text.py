import logging

import numpy as np
from pgvector.psycopg import register_vector

from rico import config
from rico.utils import get_postgres_conn

log = logging.getLogger(__name__)

_MODEL_NAME = "sentence-transformers"
_MODEL_VERSION = config.SBERT_MODEL_NAME

_UPSERT_SQL = """
INSERT INTO screens_embeddings
    (screen_id, model_name, model_version, embedding_kind, vector, run_id, source_fingerprint)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (screen_id, model_name, model_version, embedding_kind) DO UPDATE SET
    vector             = EXCLUDED.vector,
    run_id             = EXCLUDED.run_id,
    source_fingerprint = EXCLUDED.source_fingerprint
"""


def embed_text_task(**context) -> None:
    from sentence_transformers import SentenceTransformer

    run_id = context["ti"].xcom_pull(task_ids="setup_run", key="run_id")

    model = SentenceTransformer(config.SBERT_MODEL_NAME)
    log.info("SBERT loaded: %s", config.SBERT_MODEL_NAME)

    with get_postgres_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT screen_id, parsed_text, source_fingerprint FROM screens_metadata WHERE run_id = %s",
                (run_id,),
            )
            screens = cur.fetchall()

        if not screens:
            log.warning("no screens found for run_id=%s", run_id)
            return

        screen_ids   = [r[0] for r in screens]
        parsed_texts = [r[1] or "" for r in screens]
        fingerprints = [r[2] for r in screens]

        vecs_np = model.encode(parsed_texts, normalize_embeddings=True).astype("float32")

        register_vector(conn)
        with conn.cursor() as cur:
            for sid, fp, vec in zip(screen_ids, fingerprints, vecs_np):
                cur.execute(
                    _UPSERT_SQL,
                    (sid, _MODEL_NAME, _MODEL_VERSION, "text", vec, run_id, fp),
                )
                log.info(
                    "[%s] embed_text screen=%d  dim=%d  norm=%.4f  fp=%s",
                    run_id, sid, len(vec), float(np.linalg.norm(vec)), fp[:12],
                )
        conn.commit()

    log.info("[%s] embed_text complete: %d vectors", run_id, len(screens))
