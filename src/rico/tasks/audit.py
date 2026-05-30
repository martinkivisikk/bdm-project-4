import json
import logging

from airflow.exceptions import AirflowFailException

from rico import config
from rico.utils import get_postgres_conn

log = logging.getLogger(__name__)


_METADATA_DUPLICATES_SQL = """
SELECT screen_id, COUNT(*) AS n
FROM screens_metadata
WHERE run_id = %s
GROUP BY screen_id
HAVING COUNT(*) > 1
"""

_EMBEDDING_DUPLICATES_SQL = """
SELECT screen_id,
       model_name,
       model_version,
       embedding_kind,
       COUNT(*) AS n
FROM screens_embeddings
WHERE run_id = %s
GROUP BY screen_id,
         model_name,
         model_version,
         embedding_kind
HAVING COUNT(*) > 1
"""

_INSERT_AUDIT_SQL = """
INSERT INTO audit_results (run_id, audit_name, passed, details)
VALUES (%s, %s, %s, %s)
"""

_UPDATE_STATUS_SQL = """
UPDATE pipeline_runs
SET status = %s
WHERE run_id = %s
"""


def audit_task(**context) -> None:
    run_id = context["ti"].xcom_pull(task_ids="setup_run", key="run_id")

    with get_postgres_conn() as conn:
        with conn.cursor() as cur:

            # -------------------------
            # 1. Metadata duplicates
            # -------------------------
            cur.execute(_METADATA_DUPLICATES_SQL, (run_id,))
            metadata_duplicates = [
                {
                    "screen_id": screen_id,
                    "count": count,
                }
                for screen_id, count in cur.fetchall()
            ]

            # -------------------------
            # 2. Embedding duplicates
            # -------------------------
            cur.execute(_EMBEDDING_DUPLICATES_SQL, (run_id,))
            embedding_duplicates = [
                {
                    "screen_id": screen_id,
                    "model_name": model_name,
                    "model_version": model_version,
                    "embedding_kind": embedding_kind,
                    "count": count,
                }
                for (
                    screen_id,
                    model_name,
                    model_version,
                    embedding_kind,
                    count,
                ) in cur.fetchall()
            ]

        # -------------------------
        # Debug injection (test hook)
        # -------------------------
        if getattr(config, "DEBUG_FORCE_AUDIT_FAIL", False):
            metadata_duplicates = [{"screen_id": 999, "count": 2}]

        audit_passed = (
            len(metadata_duplicates) == 0
            and len(embedding_duplicates) == 0
        )

        details = {
            "duplicate_metadata": metadata_duplicates,
            "duplicate_embeddings": embedding_duplicates,
        }

        # -------------------------
        # FAILURE PATH
        # -------------------------
        if not audit_passed:
            log.error(
                "[%s] duplicate metadata rows detected: %s",
                run_id,
                json.dumps(metadata_duplicates, indent=2),
            )

            log.error(
                "[%s] duplicate embedding rows detected: %s",
                run_id,
                json.dumps(embedding_duplicates, indent=2),
            )

            with conn.cursor() as cur:
                cur.execute(
                    _INSERT_AUDIT_SQL,
                    (
                        run_id,
                        "duplicate_check",
                        False,
                        json.dumps(details),
                    ),
                )

                cur.execute(
                    _UPDATE_STATUS_SQL,
                    (
                        "audit-failed",
                        run_id,
                    ),
                )

            conn.commit()

            raise AirflowFailException(
                "Audit failed: duplicate rows detected"
            )

        # -------------------------
        # SUCCESS PATH
        # -------------------------
        with conn.cursor() as cur:
            cur.execute(
                _INSERT_AUDIT_SQL,
                (
                    run_id,
                    "duplicate_check",
                    True,
                    json.dumps(details),
                ),
            )

            cur.execute(
                _UPDATE_STATUS_SQL,
                (
                    "succeeded",
                    run_id,
                ),
            )

        conn.commit()

    log.info("[%s] audit passed", run_id)