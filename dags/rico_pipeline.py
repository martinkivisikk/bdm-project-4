from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

from rico.traceability import setup_run_task, on_dag_success, on_dag_failure
from rico.tasks.ingest import ingest_task
from rico.tasks.parse import parse_task
from rico.tasks.embed_image import embed_image_task
from rico.tasks.embed_text import embed_text_task
from rico.tasks.extract import extract_task
from rico.tasks.load import load_task
from rico.tasks.audit import audit_task
from rico.tasks.eval import eval_task

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
    ingest      = PythonOperator(task_id="ingest",      python_callable=ingest_task)
    parse       = PythonOperator(task_id="parse",       python_callable=parse_task)
    embed_image = PythonOperator(task_id="embed_image", python_callable=embed_image_task)
    embed_text  = PythonOperator(task_id="embed_text",  python_callable=embed_text_task)
    extract     = PythonOperator(task_id="extract",     python_callable=extract_task)
    load        = PythonOperator(task_id="load",        python_callable=load_task)
    audit       = PythonOperator(task_id="audit",       python_callable=audit_task)
    evaluate    = PythonOperator(task_id="eval",        python_callable=eval_task)

    setup_run >> ingest >> parse >> [embed_image, embed_text, extract] >> load >> audit >> evaluate
