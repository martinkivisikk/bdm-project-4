"""Observability helpers for RICO pipeline runs.

Metrics are persisted as key-value rows in pipeline_metrics and can be
summarized at the end of a DAG run for a quick operator inspection.
"""

from __future__ import annotations

import logging
from typing import Any

from psycopg.types.json import Jsonb

from rico.utils import get_postgres_conn

log = logging.getLogger(__name__)


def record_metric(
    run_id: str,
    metric_name: str,
    metric_value: int | float | bool,
    details: dict[str, Any] | list[Any] | None = None,
) -> None:
    """Insert one metric row for a pipeline run."""
    with get_postgres_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_metrics (run_id, metric_name, metric_value, details)
            VALUES (%s, %s, %s, %s)
            """,
            (
                run_id,
                metric_name,
                float(metric_value),
                Jsonb(details) if details is not None else None,
            ),
        )
        conn.commit()

    log.info(
        "[%s] metric recorded: %s=%s details=%s",
        run_id,
        metric_name,
        metric_value,
        details,
    )


def record_metrics(run_id: str, metrics: list[tuple[str, int | float | bool, Any | None]]) -> None:
    """Insert several metric rows in one transaction."""
    if not metrics:
        return

    with get_postgres_conn() as conn, conn.cursor() as cur:
        for metric_name, metric_value, details in metrics:
            cur.execute(
                """
                INSERT INTO pipeline_metrics (run_id, metric_name, metric_value, details)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    run_id,
                    metric_name,
                    float(metric_value),
                    Jsonb(details) if details is not None else None,
                ),
            )
        conn.commit()

    log.info("[%s] recorded %d metrics", run_id, len(metrics))


def get_run_summary(run_id: str) -> dict[str, dict[str, Any]]:
    """
    Return latest metric values for a run as a dict.

    If the same metric is inserted more than once because of retries, the newest
    row wins. The full metric history remains available in pipeline_metrics.
    """
    with get_postgres_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (metric_name)
                   metric_name,
                   metric_value,
                   details,
                   created_at
            FROM pipeline_metrics
            WHERE run_id = %s
            ORDER BY metric_name, created_at DESC, id DESC
            """,
            (run_id,),
        )
        rows = cur.fetchall()

    return {
        metric_name: {
            "value": float(metric_value),
            "details": details,
            "created_at": created_at,
        }
        for metric_name, metric_value, details, created_at in rows
    }


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _metric_value(summary: dict[str, dict[str, Any]], name: str) -> float | None:
    item = summary.get(name)
    return None if item is None else float(item["value"])


def build_run_summary_line(run_id: str) -> str:
    """Build a compact one-line summary suitable for logs and Slack."""
    summary = get_run_summary(run_id)

    screens = _metric_value(summary, "screens_ingested")
    extracted = _metric_value(summary, "screens_extracted")
    payload_pct = _metric_value(summary, "pct_extraction_non_null")
    conf_pct = _metric_value(summary, "pct_confidence_gte_0_5")
    review_pct = _metric_value(summary, "pct_in_review_queue")
    recall_at_5 = _metric_value(summary, "recall_at_5")
    duration = _metric_value(summary, "run_duration_seconds")

    parts = [f"run_id={run_id}"]

    if duration is not None:
        parts.append(f"duration={duration:.0f}s")
    if screens is not None:
        parts.append(f"ingested={screens:.0f}")
    if extracted is not None:
        parts.append(f"extracted={extracted:.0f}")
    if payload_pct is not None:
        parts.append(f"payload={_fmt_pct(payload_pct)}")
    if conf_pct is not None:
        parts.append(f"conf>=0.5={_fmt_pct(conf_pct)}")
    if review_pct is not None:
        parts.append(f"review={_fmt_pct(review_pct)}")
    if recall_at_5 is not None:
        parts.append(f"recall@5={recall_at_5:.3f}")

    image_count = _metric_value(summary, "screens_embedded_image")
    text_count = _metric_value(summary, "screens_embedded_text")
    if image_count is not None:
        parts.append(f"image_vecs={image_count:.0f}")
    if text_count is not None:
        parts.append(f"text_vecs={text_count:.0f}")

    status = summary.get("final_status")
    if status and status.get("details"):
        parts.append(f"status={status['details'].get('status')}")

    return " ".join(parts)


def log_run_summary(run_id: str) -> str:
    """
    Log a compact summary block and return the one-line summary.

    Returned value can be reused by Slack notifications.
    """
    summary_line = build_run_summary_line(run_id)
    log.info("[%s] RUN SUMMARY | %s", run_id, summary_line)
    return summary_line