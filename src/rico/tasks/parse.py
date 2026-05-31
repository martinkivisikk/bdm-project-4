import json
import logging
import time

from rico import config
from rico.observability import record_metric
from rico.utils import get_postgres_conn, get_s3_client

log = logging.getLogger(__name__)


def _parse_hierarchy(raw_json: str) -> list[tuple[str, str, tuple[int, int, int, int]]]:
    """Iterative DFS — returns (element_type, text, bounds) for nodes with text or class."""
    try:
        tree = json.loads(raw_json)
    except json.JSONDecodeError:
        return []
    root = tree.get("activity", {}).get("root", tree) if isinstance(tree, dict) else None

    elements: list[tuple[str, str, tuple[int, int, int, int]]] = []
    stack = [root]
    while stack:
        node = stack.pop()
        if not isinstance(node, dict):
            continue
        text = (node.get("text") or "").strip()
        cls = (node.get("class") or "").strip()
        if text or cls:
            element_type = cls.rsplit(".", 1)[-1] if cls else ""
            raw_bounds = node.get("bounds") or [0, 0, 0, 0]
            bounds = tuple(int(b) for b in raw_bounds) if len(raw_bounds) == 4 else (0, 0, 0, 0)
            elements.append((element_type, text, bounds))
        children = node.get("children")
        if isinstance(children, list):
            stack.extend(reversed(children))
    return elements


def _text_representation(elements: list[tuple[str, str, tuple[int, int, int, int]]]) -> str:
    """Concatenate texts in reading order: sort by (y_top, x_left), join with spaces."""
    with_text = [e for e in elements if e[1]]
    in_order = sorted(with_text, key=lambda e: (e[2][1], e[2][0]))
    return " ".join(text for _, text, _ in in_order)


def parse_task(**context) -> None:
    run_id = context["ti"].xcom_pull(task_ids="setup_run", key="run_id")
    t0 = time.time()

    s3 = get_s3_client()

    with get_postgres_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT screen_id, hierarchy_json_path FROM screens_metadata WHERE run_id = %s",
            (run_id,),
        )
        screens = cur.fetchall()

        count = 0
        for screen_id, hier_key in screens:
            raw_json = s3.get_object(Bucket=config.MINIO_BUCKET, Key=hier_key)["Body"].read().decode("utf-8")
            elements = _parse_hierarchy(raw_json)
            parsed_text = _text_representation(elements)

            cur.execute(
                "UPDATE screens_metadata SET parsed_text = %s WHERE screen_id = %s AND run_id = %s",
                (parsed_text, screen_id, run_id),
            )
            count += 1

        conn.commit()

    record_metric(run_id, "parse_seconds", time.time() - t0)
    record_metric(run_id, "screens_parsed", count)
    log.info("parse complete: %d screens", count)
