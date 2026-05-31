import json
import logging
import time

from rico.observability import record_metrics
from rico.utils import get_postgres_conn

log = logging.getLogger(__name__)

_UPSERT_EXTRACTION_SQL = """
INSERT INTO screens_metadata (
    screen_id, png_path, hierarchy_json_path,
    extraction_payload, prompt_version, confidence, run_id, updated_at
)
VALUES (%s, '', '', %s::jsonb, %s, %s, %s, NOW())
ON CONFLICT (screen_id) DO UPDATE SET
    extraction_payload = EXCLUDED.extraction_payload,
    prompt_version     = EXCLUDED.prompt_version,
    confidence         = EXCLUDED.confidence,
    run_id             = EXCLUDED.run_id,
    updated_at         = NOW()
"""


def _pct(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def load_task(**context) -> None:
    run_id = context["ti"].xcom_pull(task_ids="setup_run", key="run_id")
    t0 = time.time()

    extraction_results: dict = (
        context["ti"].xcom_pull(task_ids="extract", key="extraction_results") or {}
    )

    with get_postgres_conn() as conn, conn.cursor() as cur:
        for screen_id_str, result in extraction_results.items():
            cur.execute(
                _UPSERT_EXTRACTION_SQL,
                (
                    int(screen_id_str),
                    json.dumps(result["payload"]),
                    result["prompt_version"],
                    result["confidence"],
                    run_id,
                ),
            )
            log.info(
                "[%s] load screen=%s  conf=%.2f",
                run_id, screen_id_str, result["confidence"],
            )
        conn.commit()
        log.info("[%s] load: wrote extraction for %d screens", run_id, len(extraction_results))

    with get_postgres_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*)                                                      AS total,
                COUNT(*) FILTER (WHERE extraction_payload IS NOT NULL)        AS has_payload,
                COUNT(*) FILTER (WHERE confidence >= 0.5)                     AS high_conf,
                COUNT(DISTINCT app_package)                                    AS distinct_apps,
                COUNT(DISTINCT category)                                       AS distinct_cats
            FROM screens_metadata
            WHERE run_id = %s
            """,
            (run_id,),
        )
        total, has_payload, high_conf, distinct_apps, distinct_cats = cur.fetchone()

        cur.execute(
            "SELECT COUNT(*) FROM screens_review_queue WHERE run_id = %s",
            (run_id,),
        )
        (review_count,) = cur.fetchone()

        cur.execute(
            """
            SELECT
                se.model_version,
                se.embedding_kind,
                COUNT(*)                                                          AS n,
                AVG(vector_dims(se.vector))::int                                  AS avg_dim,
                SUM(CASE WHEN inner_product(se.vector, se.vector) = 0 THEN 1 ELSE 0 END) AS zero_vecs
            FROM screens_embeddings se
            WHERE se.run_id = %s
            GROUP BY se.model_version, se.embedding_kind
            ORDER BY se.embedding_kind, se.model_version
            """,
            (run_id,),
        )
        emb_rows = cur.fetchall()


    metrics = [
        ("load_seconds", time.time() - t0, None),
        ("screens_ingested", float(total), None),
        ("screens_extracted", float(has_payload), None),
        ("pct_extraction_non_null", _pct(has_payload, total), None),
        ("pct_confidence_gte_0_5", _pct(high_conf, total), None),
        ("pct_in_review_queue", _pct(review_count, total), None),
        ("distinct_app_packages", float(distinct_apps), None),
        ("distinct_categories", float(distinct_cats), None),
        ("load_rows_in", float(len(extraction_results)), None),
        ("load_rows_out", float(has_payload), None),
    ]

    emb_summary = []
    for model_version, kind, n, avg_dim, zero_vecs in emb_rows:
        zero_vecs = zero_vecs or 0
        zero_pct = _pct(zero_vecs, n)
        details = {"model_version": model_version, "embedding_kind": kind}

        if kind == "image":
            metrics.append(("screens_embedded_image", float(n), details))
        elif kind == "text":
            metrics.append(("screens_embedded_text", float(n), details))

        metrics.append((f"embeddings_count_{kind}", float(n), details))
        metrics.append((f"avg_vector_dims_{kind}", float(avg_dim or 0), details))
        metrics.append((f"pct_zero_norm_vectors_{kind}", zero_pct, details))

        emb_summary.append(
            f"{kind}({model_version}): n={n} dim={avg_dim} zero={zero_pct:.1%}"
        )

    record_metrics(run_id, metrics)

    log.info(
        "[%s] LOAD SUMMARY | "
        "screens=%d  payload=%.1f%%  conf≥0.5=%.1f%%  review=%.1f%%  "
        "apps=%d  cats=%d | %s",
        run_id,
        total,
        _pct(has_payload, total) * 100,
        _pct(high_conf, total) * 100,
        _pct(review_count, total) * 100,
        distinct_apps,
        distinct_cats,
        "  ".join(emb_summary) if emb_summary else "no embeddings",
    )
