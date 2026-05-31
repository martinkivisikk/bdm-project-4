import logging
import time

from sentence_transformers import SentenceTransformer

from rico import config
from rico.observability import record_metric
from rico.utils import get_postgres_conn

log = logging.getLogger(__name__)


_INSERT_EVAL_SQL = """
INSERT INTO screens_eval (
    embedding_model_version,
    n_queries,
    recall_at_5
)
VALUES (%s, %s, %s)
"""


def eval_task(**context) -> None:
    run_id = context["ti"].xcom_pull(task_ids="setup_run", key="run_id")
    t0 = time.time()

    model = SentenceTransformer(config.SBERT_MODEL_NAME)

    hits = 0
    n_queries = 0

    with get_postgres_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT screen_id, parsed_text
                FROM screens_metadata
                WHERE run_id = %s
                """,
                (run_id,),
            )
            screens = cur.fetchall()

        # -------------------------
        # DEBUG: restrict to 1 screen
        # -------------------------
        if config.DEBUG_EVAL_SINGLE_SCREEN:
            log.warning("[%s] DEBUG_EVAL_SINGLE_SCREEN enabled", run_id)
            screens = screens[:1]

        # -------------------------
        # DEBUG: guaranteed failure injection
        # -------------------------
        if config.DEBUG_FORCE_EVAL_FAIL:
            log.warning("[%s] DEBUG_FORCE_EVAL_FAIL enabled → injecting fake screen", run_id)

            # This ensures deterministic failure:
            # screen_id is extremely unlikely to ever appear in results
            screens.append(
                (-999999999, "THIS_IS_INTENTIONALLY_BROKEN_EVAL_CASE")
            )

        n_queries = len(screens)

        with conn.cursor() as cur:
            for screen_id, parsed_text in screens:
                text = parsed_text or ""

                embedding = model.encode(
                    text,
                    normalize_embeddings=True,
                )

                vector_literal = "[" + ",".join(map(str, embedding.tolist())) + "]"

                cur.execute(
                    """
                    SELECT screen_id
                    FROM screens_embeddings
                    WHERE embedding_kind = 'text'
                    ORDER BY vector <-> %s::vector
                    LIMIT 5
                    """,
                    (vector_literal,),
                )

                neighbours = [row[0] for row in cur.fetchall()]

                hit = screen_id in neighbours

                # strict debug check (optional)
                if config.DEBUG_EVAL_ASSERT_SELF:
                    if not hit:
                        log.error(
                            "[%s] SELF CHECK FAILED screen=%s top5=%s",
                            run_id,
                            screen_id,
                            neighbours,
                        )
                        raise ValueError("Eval self-check failed")

                if hit:
                    hits += 1

                log.info(
                    "[%s] eval screen=%s hit=%s top5=%s",
                    run_id,
                    screen_id,
                    hit,
                    neighbours,
                )

        recall_at_5 = hits / n_queries if n_queries else 0.0

        with conn.cursor() as cur:
            cur.execute(
                _INSERT_EVAL_SQL,
                (
                    config.SBERT_MODEL_NAME,
                    n_queries,
                    recall_at_5,
                ),
            )

        conn.commit()

    record_metric(run_id, "eval_seconds", time.time() - t0)
    record_metric(run_id, "recall_at_5", recall_at_5)
    log.info(
        "[%s] eval complete: hits=%d queries=%d recall@5=%.4f",
        run_id,
        hits,
        n_queries,
        recall_at_5,
    )