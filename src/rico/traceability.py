"""
pipeline_runs lifecycle management.

Public surface used by the DAG and tasks:
  setup_run_task(**context)       — Airflow PythonOperator callable, creates the run row,
                                    pushes run_id to XCom under key "run_id".
  on_dag_success(context)         — DAG-level on_success_callback.
  on_dag_failure(context)         — DAG-level on_failure_callback.
  finish_run(run_id, status)      — Called directly by audit task to set "paused-by-audit"
                                    before raising AirflowFailException.

Downstream tasks retrieve run_id via:
  run_id = context["ti"].xcom_pull(task_ids="setup_run", key="run_id")
"""

import logging
import subprocess
import uuid

import psycopg

from rico import config
from rico.notifications import notify_run_finished, notify_run_started

log = logging.getLogger(__name__)

# Statuses that should not be overwritten by the generic failure callback.
_TERMINAL_STATUSES = {"succeeded", "failed", "paused-by-audit"}


# Internal helpers 

def _get_git_sha() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return None


def _get_run_id_for_dag_run(dag_run_id: str) -> str | None:
    """Look up the run_id created by setup_run for this Airflow dag_run_id."""
    with psycopg.connect(config.POSTGRES_DSN) as conn:
        row = conn.execute(
            "SELECT run_id FROM pipeline_runs WHERE dag_run_id = %s LIMIT 1",
            (dag_run_id,),
        ).fetchone()
    return str(row[0]) if row else None


def _get_run_finish_info(run_id: str) -> tuple[str, float | None]:
    """Return final status and duration seconds for a pipeline run."""
    with psycopg.connect(config.POSTGRES_DSN) as conn:
        row = conn.execute(
            """
            SELECT
                status,
                EXTRACT(EPOCH FROM (COALESCE(ended_at, NOW()) - started_at)) AS duration_seconds
            FROM pipeline_runs
            WHERE run_id = %s
            """,
            (run_id,),
        ).fetchone()

    if not row:
        return "failed", None

    return str(row[0]), float(row[1]) if row[1] is not None else None


def _trigger_type(context) -> str:
    """Map Airflow DAG run metadata to manual/scheduled wording."""
    dag_run = context.get("dag_run")
    run_type = str(getattr(dag_run, "run_type", "") or "").lower()

    if run_type == "scheduled":
        return "scheduled"
    if run_type == "manual":
        return "manual"
    if getattr(dag_run, "external_trigger", False):
        return "manual"

    return "scheduled"


# Core DB operations 

def create_run(dag_run_id: str, limit_param: int) -> str:
    """Insert a new pipeline_runs row and return its run_id (UUID string)."""
    run_id = str(uuid.uuid4())
    clip_version = f"{config.CLIP_MODEL_NAME}/{config.CLIP_MODEL_PRETRAINED}"
    with psycopg.connect(config.POSTGRES_DSN) as conn:
        conn.execute(
            """
            INSERT INTO pipeline_runs
                (run_id, dag_run_id, limit_param, git_sha,
                 clip_version, sbert_version, llm_model, prompt_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                dag_run_id,
                limit_param,
                _get_git_sha(),
                clip_version,
                config.SBERT_MODEL_NAME,
                config.OLLAMA_MODEL,
                config.PROMPT_VERSION,
            ),
        )
    log.info("Created pipeline run %s (dag_run_id=%s, limit=%d)", run_id, dag_run_id, limit_param)
    return run_id


def finish_run(run_id: str, status: str) -> None:
    """
    Set final status and ended_at on a pipeline_runs row.
    Safe to call from the audit task with "paused-by-audit" before raising.
    """
    with psycopg.connect(config.POSTGRES_DSN) as conn:
        conn.execute(
            "UPDATE pipeline_runs SET status = %s, ended_at = NOW() WHERE run_id = %s",
            (status, run_id),
        )
    log.info("Pipeline run %s finished with status=%s", run_id, status)


# Airflow callables 

def setup_run_task(**context) -> str:
    """
    PythonOperator callable for the setup_run task.
    Creates the pipeline_runs row and pushes run_id to XCom.
    """
    dag_run_id = context["dag_run"].run_id
    limit_param = context["params"].get("LIMIT", 5)
    run_id = create_run(dag_run_id, limit_param)
    context["ti"].xcom_push(key="run_id", value=run_id)

    notify_run_started(run_id, int(limit_param), _trigger_type(context))
    return run_id


def on_dag_success(context) -> None:
    """DAG on_success_callback — marks the run as succeeded."""
    dag_run_id = context["dag_run"].run_id
    run_id = _get_run_id_for_dag_run(dag_run_id)
    if run_id:
        finish_run(run_id, "succeeded")
        status, duration_sec = _get_run_finish_info(run_id)

        # issue #7 can later replace it
        notify_run_finished(run_id, status, duration_sec, "pipeline completed")


def on_dag_failure(context) -> None:
    """
    DAG on_failure_callback — marks the run as failed, but only if it has not
    already been moved to a terminal status (e.g. "paused-by-audit" by the
    audit task before it raised AirflowFailException).
    """
    dag_run_id = context["dag_run"].run_id
    run_id = _get_run_id_for_dag_run(dag_run_id)
    if not run_id:
        return
    with psycopg.connect(config.POSTGRES_DSN) as conn:
        conn.execute(
            """
            UPDATE pipeline_runs
               SET status = 'failed', ended_at = NOW()
             WHERE run_id = %s AND status = 'running'
            """,
            (run_id,),
        )
    log.info("Pipeline run %s marked failed (if still running)", run_id)

    status, duration_sec = _get_run_finish_info(run_id)

    task_instance = context.get("task_instance")
    failed_task_id = getattr(task_instance, "task_id", "unknown")

    summary = (
        "audit circuit breaker halted pipeline"
        if status == "paused-by-audit"
        else f"task={failed_task_id}"
    )

    notify_run_finished(run_id, status, duration_sec, summary)
