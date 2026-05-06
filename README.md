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

After each successful run, `pipeline_metrics` holds one row per metric keyed by `run_id`. Query with:

```sql
SELECT metric_name, metric_value, details
FROM pipeline_metrics
WHERE run_id = '<your-run-id>'
ORDER BY metric_name;
```

### Health metrics

| `metric_name`           | Meaning                                                              |
|-------------------------|----------------------------------------------------------------------|
| `run_duration_s`        | Total wall-clock time for the DAG run in seconds                     |
| `task_duration_s`       | Per-task duration; `details` contains `{"task_id": "..."}` to identify which task |
| `retry_count`           | Number of task retries across the run                                |
| `final_status`          | `1.0` = succeeded, `0.0` = failed / paused-by-audit                 |

### Data quality metrics

| `metric_name`              | Meaning                                                           |
|----------------------------|-------------------------------------------------------------------|
| `screens_ingested`         | Rows inserted or updated in `screens_metadata`                    |
| `screens_embedded_image`   | Rows inserted in `screens_embeddings` with `embedding_kind=image` |
| `screens_embedded_text`    | Rows inserted in `screens_embeddings` with `embedding_kind=text`  |
| `screens_extracted`        | Screens with a non-null `extraction_payload` after the extract task |
| `screens_review_queue`     | Screens routed to `screens_review_queue` due to LLM parse failure |
| `metadata_completeness`    | Fraction of ingested screens that have a non-null `extraction_payload` |
| `confidence_mean`          | Mean extraction confidence across all processed screens           |
| `confidence_p25` / `p75`   | 25th / 75th percentile of extraction confidence                   |
| `zero_vectors_image`       | Count of all-zero image embedding vectors (indicates CLIP failure) |
| `zero_vectors_text`        | Count of all-zero text embedding vectors (indicates SBERT failure) |
| `distinct_apps`            | Number of distinct `app_package` values in the current batch      |
| `distinct_categories`      | Number of distinct `category` values in the current batch         |
| `recall_at_5`              | recall@5 from the eval task (higher is better; baseline ~0.6)     |

A healthy run should show `metadata_completeness ≥ 0.9`, `zero_vectors_* = 0`, and `screens_review_queue` near zero.

## Interpreting audit failures

The `audit` task is a circuit breaker: if it detects data quality problems it writes a failure row to `audit_results`, sets the pipeline run status to `paused-by-audit`, and halts the DAG before `eval` runs.

Check what failed:

```sql
SELECT audit_name, passed, details
FROM audit_results
WHERE run_id = '<your-run-id>';
```

| `audit_name`                  | What it checks                                                    | What to do when it fails |
|-------------------------------|-------------------------------------------------------------------|--------------------------|
| `no_duplicate_embeddings`     | No duplicate `(screen_id, model_name, model_version, embedding_kind)` in `screens_embeddings` | Run `make reset` and re-trigger, or inspect `details` for the duplicate keys and delete them manually |
| `no_duplicate_metadata`       | No duplicate `screen_id` in `screens_metadata` for the current run | Same as above — usually caused by re-triggering without resetting |

The `details` JSONB column contains the offending keys, e.g.:

```json
{
  "duplicate_count": 3,
  "example_keys": [
    {"screen_id": 12345, "model_name": "ViT-B-32", "embedding_kind": "image"}
  ]
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
