import json
import logging

import requests

from rico import config
from rico.utils import get_postgres_conn

log = logging.getLogger(__name__)

_PROMPT_V1 = """\
You are a UI structure extractor for Android app screenshots.

Given the visible text from one screen's view hierarchy, return a single \
JSON object with these fields:

- "title": a short string naming the screen (e.g. "Login", "Settings", \
  "Search results"). Empty string if unclear.
- "elements": a list of {"type": string, "text": string} objects, one \
  per salient interactive or informational element you can identify.
- "confidence": a number in [0.0, 1.0] expressing how confident you are \
  in the extraction.

Visible text:
{hierarchy_text}

Respond with valid JSON only — no commentary, no Markdown fences.
"""

_REVIEW_SQL = """
INSERT INTO screens_review_queue (screen_id, reason, raw_output, run_id)
VALUES (%s, %s, %s, %s)
"""


def _call_ollama(text: str) -> dict:
    prompt = _PROMPT_V1.replace("{hierarchy_text}", text)
    resp = requests.post(
        f"{config.OLLAMA_BASE_URL}/api/generate",
        json={"model": config.OLLAMA_MODEL, "prompt": prompt, "stream": False},
        timeout=120,
    )
    resp.raise_for_status()
    return json.loads(resp.json()["response"])


def extract_task(**context) -> None:
    run_id = context["ti"].xcom_pull(task_ids="setup_run", key="run_id")

    results: dict[str, dict] = {}  # str(screen_id) → {payload, prompt_version, confidence}
    succeeded = 0
    failed = 0

    with get_postgres_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT screen_id, parsed_text FROM screens_metadata WHERE run_id = %s",
                (run_id,),
            )
            screens = cur.fetchall()

        with conn.cursor() as cur:
            for screen_id, parsed_text in screens:
                text = parsed_text or ""
                try:
                    payload = _call_ollama(text)
                    confidence = float(payload.get("confidence", 0.0))
                    if confidence < 0.5:
                        cur.execute(_REVIEW_SQL, (screen_id, "low_confidence", json.dumps(payload), run_id))
                        log.warning(
                            "[%s] extract screen=%d  conf=%.2f < 0.5 → review_queue",
                            run_id, screen_id, confidence,
                        )
                        failed += 1
                        continue
                    body = {k: v for k, v in payload.items() if k != "confidence"}
                    results[str(screen_id)] = {
                        "payload": body,
                        "prompt_version": config.PROMPT_VERSION,
                        "confidence": confidence,
                    }
                    log.info(
                        "[%s] extract screen=%d  conf=%.2f  title=%r",
                        run_id, screen_id, confidence, body.get("title", ""),
                    )
                    succeeded += 1
                except (json.JSONDecodeError, requests.RequestException, KeyError) as exc:
                    cur.execute(_REVIEW_SQL, (screen_id, "invalid_json", str(exc), run_id))
                    log.warning(
                        "[%s] extract screen=%d  routed to review_queue: %s",
                        run_id, screen_id, exc,
                    )
                    failed += 1

        conn.commit()

    context["ti"].xcom_push(key="extraction_results", value=results)
    log.info(
        "[%s] extract complete: %d succeeded  %d failed → review_queue",
        run_id, succeeded, failed,
    )
