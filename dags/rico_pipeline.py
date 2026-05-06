from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

from rico.traceability import setup_run_task, on_dag_success, on_dag_failure

log = logging.getLogger(__name__)


def _stub(**kwargs):
    task_id = kwargs["ti"].task_id
    log.info("Task '%s' not yet implemented — skipping", task_id)


default_args = {"retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="rico_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,          # manual trigger for dev; set to @daily for prod
    catchup=False,
    params={"LIMIT": 5},    # override at trigger time
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
) as dag:

    setup_run   = PythonOperator(task_id="setup_run",   python_callable=setup_run_task)
    ingest      = PythonOperator(task_id="ingest",      python_callable=_stub)
    parse       = PythonOperator(task_id="parse",       python_callable=_stub)
    embed_image = PythonOperator(task_id="embed_image", python_callable=_stub)
    embed_text  = PythonOperator(task_id="embed_text",  python_callable=_stub)
    extract     = PythonOperator(task_id="extract",     python_callable=_stub)
    load        = PythonOperator(task_id="load",        python_callable=_stub)
    audit       = PythonOperator(task_id="audit",       python_callable=_stub)
    evaluate    = PythonOperator(task_id="eval",        python_callable=_stub)

    setup_run >> ingest >> parse >> [embed_image, embed_text, extract] >> load >> audit >> evaluate
