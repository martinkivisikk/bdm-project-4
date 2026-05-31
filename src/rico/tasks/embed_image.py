import logging
import time
from io import BytesIO

import numpy as np
from pgvector.psycopg import register_vector

from rico import config
from rico.observability import record_metric
from rico.utils import get_postgres_conn, get_s3_client

log = logging.getLogger(__name__)

_MODEL_NAME = "open-clip"
_MODEL_VERSION = (
    f"open-clip-{config.CLIP_MODEL_NAME}-{config.CLIP_MODEL_PRETRAINED.replace('_', '-')}"
)

_UPSERT_SQL = """
INSERT INTO screens_embeddings
    (screen_id, model_name, model_version, embedding_kind, vector, run_id, source_fingerprint)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (screen_id, model_name, model_version, embedding_kind) DO UPDATE SET
    vector             = EXCLUDED.vector,
    run_id             = EXCLUDED.run_id,
    source_fingerprint = EXCLUDED.source_fingerprint
"""


def embed_image_task(**context) -> None:
    import open_clip
    import torch
    from PIL import Image

    run_id = context["ti"].xcom_pull(task_ids="setup_run", key="run_id")
    t0 = time.time()

    model, _, preprocess = open_clip.create_model_and_transforms(
        config.CLIP_MODEL_NAME, pretrained=config.CLIP_MODEL_PRETRAINED
    )
    model.eval()
    log.info("CLIP loaded: %s / %s", config.CLIP_MODEL_NAME, config.CLIP_MODEL_PRETRAINED)

    s3 = get_s3_client()

    with get_postgres_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT screen_id, png_path, source_fingerprint FROM screens_metadata WHERE run_id = %s",
                (run_id,),
            )
            screens = cur.fetchall()

        if not screens:
            log.warning("no screens found for run_id=%s", run_id)
            return

        screen_ids = [r[0] for r in screens]
        png_paths  = [r[1] for r in screens]
        fingerprints = [r[2] for r in screens]

        batch = []
        for png_path in png_paths:
            blob = s3.get_object(Bucket=config.MINIO_BUCKET, Key=png_path)["Body"].read()
            img = Image.open(BytesIO(blob)).convert("RGB")
            batch.append(preprocess(img))

        images_tensor = torch.stack(batch)
        with torch.no_grad():
            vecs = model.encode_image(images_tensor)
            vecs = vecs / vecs.norm(dim=-1, keepdim=True)
        vecs_np = vecs.cpu().numpy().astype("float32")

        register_vector(conn)
        with conn.cursor() as cur:
            for sid, fp, vec in zip(screen_ids, fingerprints, vecs_np):
                cur.execute(
                    _UPSERT_SQL,
                    (sid, _MODEL_NAME, _MODEL_VERSION, "image", vec, run_id, fp),
                )
                log.info(
                    "[%s] embed_image screen=%d  dim=%d  norm=%.4f",
                    run_id, sid, len(vec), float(np.linalg.norm(vec)),
                )
        conn.commit()

    record_metric(run_id, "embed_image_seconds", time.time() - t0)
    log.info("[%s] embed_image complete: %d vectors", run_id, len(screens))
