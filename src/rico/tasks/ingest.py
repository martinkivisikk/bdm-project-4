import itertools
import logging
from io import BytesIO

from datasets import load_dataset

from rico import config
from rico.utils import compute_fingerprint, get_postgres_conn, get_s3_client

log = logging.getLogger(__name__)

_DATASET = "rootsautomation/RICO-Screen2Words"

_UPSERT_SQL = """
INSERT INTO screens_metadata
    (screen_id, app_package, category, png_path, hierarchy_json_path,
     run_id, source_fingerprint, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (screen_id) DO UPDATE SET
    app_package          = EXCLUDED.app_package,
    category             = EXCLUDED.category,
    png_path             = EXCLUDED.png_path,
    hierarchy_json_path  = EXCLUDED.hierarchy_json_path,
    run_id               = EXCLUDED.run_id,
    source_fingerprint   = EXCLUDED.source_fingerprint,
    updated_at           = NOW()
"""


def ingest_task(**context) -> None:
    run_id = context["ti"].xcom_pull(task_ids="setup_run", key="run_id")
    limit = context["params"].get("LIMIT", 5)

    s3 = get_s3_client()
    ds = load_dataset(_DATASET, split="train", streaming=True)

    with get_postgres_conn() as conn, conn.cursor() as cur:
        for row in itertools.islice(ds, limit):
            sid = int(row["screenId"])
            png_key = f"screens/{sid}.png"
            hier_key = f"screens/{sid}.json"

            png_buf = BytesIO()
            row["image"].save(png_buf, format="PNG")
            png_bytes = png_buf.getvalue()
            hier_bytes = row["view_hierarchy"].encode("utf-8")

            fingerprint = compute_fingerprint(png_bytes)

            s3.put_object(Bucket=config.MINIO_BUCKET, Key=png_key, Body=png_bytes)
            s3.put_object(Bucket=config.MINIO_BUCKET, Key=hier_key, Body=hier_bytes)

            cur.execute(
                _UPSERT_SQL,
                (sid, row["app_package_name"], row["category"],
                 png_key, hier_key, run_id, fingerprint),
            )
            log.info(
                "ingested screen %d  png=%dB  json=%dB  fingerprint=%s",
                sid, len(png_bytes), len(hier_bytes), fingerprint[:12],
            )

        conn.commit()

    log.info("ingest complete: %d screens", limit)
