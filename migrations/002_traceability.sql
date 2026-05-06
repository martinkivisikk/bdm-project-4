-- Production traceability, audit, and observability tables.
-- Runs after 001_init.sql (which creates the base tables).

\c rico

-- ============================================================
-- 1. pipeline_runs — one row per DAG execution
-- ============================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id              UUID PRIMARY KEY,
    dag_run_id          TEXT NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at            TIMESTAMPTZ,
    status              TEXT NOT NULL DEFAULT 'running',    -- running | succeeded | failed | paused-by-audit
    limit_param         INTEGER NOT NULL,
    git_sha             TEXT,
    clip_version        TEXT NOT NULL,
    sbert_version       TEXT NOT NULL,
    llm_model           TEXT NOT NULL,
    prompt_version      TEXT NOT NULL
);

-- ============================================================
-- 2. Add run_id + source_fingerprint to existing tables
-- ============================================================
ALTER TABLE screens_metadata
    ADD COLUMN IF NOT EXISTS run_id              UUID REFERENCES pipeline_runs(run_id),
    ADD COLUMN IF NOT EXISTS source_fingerprint  TEXT,
    -- Output staging for the parse task. Written by parse, read by embed_text and extract
    -- so those tasks stay independent and don't hit XCom size limits.
    ADD COLUMN IF NOT EXISTS parsed_text         TEXT;

ALTER TABLE screens_embeddings
    ADD COLUMN IF NOT EXISTS run_id              UUID REFERENCES pipeline_runs(run_id),
    ADD COLUMN IF NOT EXISTS source_fingerprint  TEXT;

ALTER TABLE screens_review_queue
    ADD COLUMN IF NOT EXISTS run_id              UUID REFERENCES pipeline_runs(run_id);

-- ============================================================
-- 3. audit_results — one row per audit check per run
-- ============================================================
CREATE TABLE IF NOT EXISTS audit_results (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL REFERENCES pipeline_runs(run_id),
    audit_name  TEXT NOT NULL,
    passed      BOOLEAN NOT NULL,
    details     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 4. pipeline_metrics — key-value metrics per run
-- ============================================================
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id            BIGSERIAL PRIMARY KEY,
    run_id        UUID NOT NULL REFERENCES pipeline_runs(run_id),
    metric_name   TEXT NOT NULL,
    metric_value  DOUBLE PRECISION NOT NULL,
    details       JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);