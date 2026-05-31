# RICO Pipeline

An Apache Airflow pipeline that ingests mobile UI screens from HuggingFace, stores raw artefacts in MinIO, generates CLIP image embeddings and SBERT text embeddings, extracts structured metadata via an Ollama LLM, and evaluates retrieval quality with recall@k, all backed by PostgreSQL + pgvector.

## Prerequisites

- Docker with the Compose plugin (`docker compose version` ≥ 2.1)
- ~6 GB free disk (Airflow image ~2 GB, Ollama model ~1.9 GB, data)

## Quickstart

```bash
cp .env.example .env   # edit if you need non-default credentials
make up                # builds images, starts all services (~3 min first time)
make pull-models       # pulls qwen2.5:3b into Ollama (~1.9 GB, run once)
```

Open **http://localhost:8080** (admin / admin), find `rico_pipeline`, and trigger it manually. Use the **Trigger w/ config** button to override `LIMIT` (default 5 screens).

To stop and preserve data:
```bash
make down
```

To wipe everything and start fresh:
```bash
make clean && make up && make pull-models
```

## Services

| Service       | URL / address             | Credentials                          |
|---------------|---------------------------|--------------------------------------|
| Airflow UI    | http://localhost:8080     | admin / admin                        |
| MinIO console | http://localhost:9001     | minioadmin / minioadmin              |
| MinIO S3 API  | http://localhost:9000     | bucket: `rico-raw`                   |
| PostgreSQL    | localhost:5432            | rico / rico — databases: `rico`, `airflow` |
| Ollama        | http://localhost:11434    | model: `qwen2.5:3b`                  |

## Make targets

| Target         | What it does                                                         |
|----------------|----------------------------------------------------------------------|
| `make up`      | Build images and start all services                                  |
| `make build`   | Rebuild the Airflow image (run after changing `Dockerfile` or `pyproject.toml`) |
| `make pull-models` | Pull `qwen2.5:3b` into Ollama (~1.9 GB, one-time)               |
| `make down`    | Stop all containers, keep volumes                                    |
| `make clean`   | Stop all containers and delete all volumes (full reset)              |
| `make reset`   | Truncate all DB tables and clear the MinIO bucket (lighter than clean) |
| `make logs`    | Tail logs from all containers                                        |

## Pipeline metrics

After each run, `pipeline_metrics` holds one row per metric keyed by `run_id`. Query with:

```sql
SELECT metric_name, metric_value, details
FROM pipeline_metrics
WHERE run_id = '<your-run-id>'
ORDER BY metric_name;
```

### Health metrics

| `metric_name`           | Meaning                                                              |
|-------------------------|----------------------------------------------------------------------|
| `run_duration_seconds`  | Total wall-clock time for the DAG run in seconds                     |
| `final_status`          | `1.0` = succeeded, `0.0` = failed / audit-failed                    |
| `ingest_seconds`        | Wall-clock time for the ingest task                                  |
| `parse_seconds`         | Wall-clock time for the parse task                                   |
| `embed_image_seconds`   | Wall-clock time for the embed_image task                             |
| `embed_text_seconds`    | Wall-clock time for the embed_text task                              |
| `extract_seconds`       | Wall-clock time for the extract task                                 |
| `load_seconds`          | Wall-clock time for the load task                                    |
| `audit_seconds`         | Wall-clock time for the audit task                                   |
| `eval_seconds`          | Wall-clock time for the eval task                                    |

### Data quality metrics

| `metric_name`                    | Meaning                                                                    |
|----------------------------------|----------------------------------------------------------------------------|
| `screens_ingested`               | Rows in `screens_metadata` for this run                                    |
| `screens_parsed`                 | Screens whose `parsed_text` was written by the parse task                  |
| `screens_extracted`              | Screens with a non-null `extraction_payload` after load                    |
| `screens_review_queued`          | Screens routed to `screens_review_queue` due to LLM parse failure          |
| `load_rows_in`                   | Extraction results received by the load task from XCom                     |
| `load_rows_out`                  | Rows actually written to `screens_metadata` by load                        |
| `screens_embedded_image`         | Rows in `screens_embeddings` with `embedding_kind=image` for this run      |
| `screens_embedded_text`          | Rows in `screens_embeddings` with `embedding_kind=text` for this run       |
| `embeddings_count_image`         | Same as `screens_embedded_image`; `details` includes model version         |
| `embeddings_count_text`          | Same as `screens_embedded_text`; `details` includes model version          |
| `avg_vector_dims_image`          | Mean vector dimensionality for image embeddings (should be 512)            |
| `avg_vector_dims_text`           | Mean vector dimensionality for text embeddings (should be 384)             |
| `pct_zero_norm_vectors_image`    | Fraction of image vectors whose norm is zero (silent CLIP failure)         |
| `pct_zero_norm_vectors_text`     | Fraction of text vectors whose norm is zero (silent SBERT failure)         |
| `pct_extraction_non_null`        | Fraction of ingested screens that have a non-null `extraction_payload`     |
| `pct_confidence_gte_0_5`         | Fraction of ingested screens whose LLM confidence is ≥ 0.5                |
| `pct_in_review_queue`            | Fraction of ingested screens that were routed to `screens_review_queue`    |
| `distinct_app_packages`          | Number of distinct `app_package` values in the current batch               |
| `distinct_categories`            | Number of distinct `category` values in the current batch                  |
| `recall_at_5`                    | recall@5 from the eval task (higher is better; baseline ~1.0 on 5 screens) |

A healthy run should show `pct_extraction_non_null ≥ 0.9`, `pct_zero_norm_vectors_* = 0`, and `pct_in_review_queue` near zero.

## Interpreting audit failures

The `audit` task is a circuit breaker: if it detects data quality problems it writes a failure row to `audit_results`, sets the pipeline run status to `audit-failed`, and halts the DAG before `eval` runs.

Check what failed:

```sql
SELECT audit_name, passed, details
FROM audit_results
WHERE run_id = '<your-run-id>';
```

The audit runs one check (`audit_name = duplicate_check`) that covers both tables:

| What it checks | Failure condition |
|---|---|
| No duplicate `screen_id` in `screens_metadata` for the current run | Same screen ingested more than once |
| No duplicate `(screen_id, model_name, model_version, embedding_kind)` in `screens_embeddings` | Same embedding written more than once |

The `details` JSONB column contains the offending rows for both checks, e.g.:

```json
{
  "duplicate_metadata": [{"screen_id": 12345, "count": 2}],
  "duplicate_embeddings": [{"screen_id": 12345, "model_name": "open-clip", "model_version": "open-clip-ViT-B-32-laion2b-s34b-b79k", "embedding_kind": "image", "count": 2}]
}
```

After fixing the underlying data, re-trigger the DAG. Because all writes are idempotent (`INSERT … ON CONFLICT DO UPDATE`), a clean re-run will overwrite the bad rows rather than duplicate them.

## Pipeline run history

```sql
SELECT run_id, dag_run_id, started_at, ended_at, status, limit_param
FROM pipeline_runs
ORDER BY started_at DESC
LIMIT 10;
```

## Troubleshooting

**Airflow UI shows an import error on `rico_pipeline`.**
The `src/` directory is bind-mounted into the Airflow containers. If a module under `src/rico/` has a syntax error, the DAG will fail to import. Check the scheduler logs: `make logs`.

**`make reset` fails saying a table doesn't exist.**
The traceability tables (`pipeline_runs`, `audit_results`, `pipeline_metrics`) are created by `migrations/002_traceability.sql`, which only runs on first Postgres volume init. If you started from the lab's old volume, run `make clean && make up` to re-run migrations.

**Ollama calls time out during `extract`.**
The `qwen2.5:3b` model must be pulled first (`make pull-models`). Check that the model is loaded: `docker compose exec ollama ollama list`.

**CLIP weights download slowly on first `embed_image` run.**
Open-clip downloads ~600 MB to the Hugging Face cache inside the Airflow container. This is a one-time cost per volume. Subsequent runs use the cached weights.
