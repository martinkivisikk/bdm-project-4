.PHONY: help up build down clean pull-models reset logs

COMPOSE := docker compose

OLLAMA_MODEL     ?= qwen2.5:3b
POSTGRES_USER    ?= rico
POSTGRES_DB      ?= rico
MINIO_ACCESS_KEY ?= minioadmin
MINIO_SECRET_KEY ?= minioadmin
MINIO_BUCKET     ?= rico-raw

help:
	@echo "Targets:"
	@echo "  up           build images and start all services"
	@echo "  build        rebuild the Airflow image (after Dockerfile or dep changes)"
	@echo "  pull-models  pull qwen2.5:3b into Ollama (~1.9 GB, run once)"
	@echo "  down         stop services (volumes preserved)"
	@echo "  clean        stop services and wipe all volumes (full reset)"
	@echo "  reset        truncate all DB tables + clear MinIO bucket (lighter than clean)"
	@echo "  logs         tail docker-compose logs"

up:
	$(COMPOSE) build airflow-init airflow-webserver airflow-scheduler
	$(COMPOSE) up -d --wait postgres minio ollama
	$(COMPOSE) up -d minio-init ollama-init
	$(COMPOSE) run --rm airflow-init
	$(COMPOSE) up -d airflow-webserver airflow-scheduler
	@echo ""
	@echo "  Airflow:     http://localhost:8080  (admin / admin)"
	@echo "  MinIO:       http://localhost:9001  (minioadmin / minioadmin)"
	@echo "  Postgres:    localhost:5432          (rico / rico, db: rico)"
	@echo "  Ollama:      http://localhost:11434"

build:
	$(COMPOSE) build airflow-init airflow-webserver airflow-scheduler

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

pull-models:
	$(COMPOSE) exec ollama ollama pull $(OLLAMA_MODEL)

reset:
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
	  "TRUNCATE TABLE screens_metadata, screens_embeddings, screens_review_queue, screens_eval, \
	   pipeline_runs, audit_results, pipeline_metrics RESTART IDENTITY CASCADE;"
	$(COMPOSE) exec minio mc alias set local http://minio:9000 $(MINIO_ACCESS_KEY) $(MINIO_SECRET_KEY) >/dev/null
	$(COMPOSE) exec minio mc rm --recursive --force local/$(MINIO_BUCKET)/ >/dev/null 2>&1 || true
	@echo "state truncated"

logs:
	$(COMPOSE) logs -f --tail=100
