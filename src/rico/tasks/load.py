import json
import logging

from rico.utils import get_postgres_conn

log = logging.getLogger(__name__)

_UPSERT_EXTRACTION_SQL = """
UPDATE screens_metadata
SET extraction_payload = %s::jsonb,
    prompt_version     = %s,
    confidence         = %s,
    updated_at         = NOW()
WHERE screen_id = %s AND run_id = %s
"""

_INSERT_METRIC = """
INSERT INTO pipeline_metrics (run_id, metric_name, metric_value, details)
VALUES (%s, %s, %s, %s)
"""


def _pct(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def load_task(**context) -> None:
    run_id = context["ti"].xcom_pull(task_ids="setup_run", key="run_id")

    extraction_results: dict = (
        context["ti"].xcom_pull(task_ids="extract", key="extraction_results") or {}
    )

    with get_postgres_conn() as conn, conn.cursor() as cur:
        for screen_id_str, result in extraction_results.items():
            cur.execute(
                _UPSERT_EXTRACTION_SQL,
                (
                    json.dumps(result["payload"]),
                    result["prompt_version"],
                    result["confidence"],
                    int(screen_id_str),
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

        meta_metrics = [
            ("screens_ingested",        float(total),                None),
            ("extraction_completeness", _pct(has_payload, total),    None),
            ("confidence_gte_0_5_pct",  _pct(high_conf, total),     None),
            ("review_queue_pct",        _pct(review_count, total),   None),
            ("distinct_apps",           float(distinct_apps),         None),
            ("distinct_categories",     float(distinct_cats),         None),
        ]
        for name, value, details in meta_metrics:
            cur.execute(_INSERT_METRIC, (run_id, name, value, details))

        emb_summary = []
        for model_version, kind, n, avg_dim, zero_vecs in emb_rows:
            zero_pct = _pct(zero_vecs, n)
            slug = kind  # 'image' or 'text'
            details = json.dumps({"model_version": model_version})
            cur.execute(_INSERT_METRIC, (run_id, f"embeddings_count_{slug}", float(n),       details))
            cur.execute(_INSERT_METRIC, (run_id, f"avg_vector_dim_{slug}",   float(avg_dim), details))
            cur.execute(_INSERT_METRIC, (run_id, f"zero_vector_pct_{slug}",  zero_pct,       details))
            emb_summary.append(
                f"{kind}({model_version}): n={n} dim={avg_dim} zero={zero_pct:.1%}"
            )

        conn.commit()

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
