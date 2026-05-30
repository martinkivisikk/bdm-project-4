"""Slack notifications for RICO pipeline runs.

Posts lifecycle messages to a Slack incoming webhook.

Notification failures must never fail the pipeline. Therefore, all public
functions catch and log errors instead of raising.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence
from typing import Any

import requests

log = logging.getLogger(__name__)

_WEBHOOK_ENV_VAR = "SLACK_WEBHOOK_URL"
_TIMEOUT_SECONDS = 10


def _get_webhook_url() -> str | None:
    """Return configured Slack webhook URL, or None if notifications are disabled."""
    webhook_url = os.getenv(_WEBHOOK_ENV_VAR)
    if not webhook_url:
        log.warning("%s is not set; skipping Slack notification", _WEBHOOK_ENV_VAR)
        return None
    return webhook_url


def _post_message(text: str) -> None:
    """Posting one Slack message. Never raising to callers."""

    log.info("Attempting Slack notification: %s", text)

    webhook_url = _get_webhook_url()
    if not webhook_url:
        return

    try:
        response = requests.post(
            webhook_url,
            json={"text": text},
            timeout=_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        log.info("Slack notification sent")
    except Exception as exc:  # must never crash task/DAG
        log.exception("Failed to send Slack notification: %s", exc)


def _format_sequence(values: Sequence[Any] | None) -> str:
    """Formatting duplicate key lists fully, without truncating."""
    if not values:
        return "[]"
    return "[" + ", ".join(repr(value) for value in values) + "]"


def notify_run_started(run_id: str, limit: int, trigger_type: str) -> None:
    """Notifying Slack that a RICO DAG run has started."""
    message = (
        ":large_green_circle: RICO run started | "
        f"run_id={run_id} | LIMIT={limit} | trigger={trigger_type}"
    )
    _post_message(message)


def notify_audit_halt(
    run_id: str,
    duplicate_metadata: Sequence[Any] | None,
    duplicate_embeddings: Sequence[Any] | None,
) -> None:
    """Notifying Slack that duplicate-detection audit halted the pipeline."""
    message = (
        ":rotating_light: RICO audit FAILED | "
        f"run_id={run_id} | "
        f"duplicate metadata screen_ids={_format_sequence(duplicate_metadata)} | "
        f"duplicate embedding keys={_format_sequence(duplicate_embeddings)}"
    )
    _post_message(message)


def notify_run_finished(
    run_id: str,
    status: str,
    duration_seconds: int | float | None,
    summary: str,
) -> None:
    """Notifying Slack that a RICO DAG run has finished."""
    duration_text = "unknown" if duration_seconds is None else f"{int(round(duration_seconds))}s"

    if status == "succeeded":
        icon = ":white_check_mark:"
        status_text = "succeeded"
    elif status == "paused-by-audit":
        icon = ":rotating_light:"
        status_text = "paused-by-audit"
    else:
        icon = ":red_circle:"
        status_text = "FAILED"

    message = (
        f"{icon} RICO run {status_text} | "
        f"run_id={run_id} | duration={duration_text} | {summary}"
    )
    _post_message(message)