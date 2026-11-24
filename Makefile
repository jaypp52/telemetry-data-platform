# One entry point for the whole stack. Run `make help` for the list.
SHELL := /bin/bash
COMPOSE ?= docker compose
EXEC := $(COMPOSE) exec -T airflow-scheduler

export AIRFLOW_UID ?= $(shell id -u)
# start_date of the hourly DAG: the day the stack comes up (see .env.example)
export LIVE_START_DATE ?= $(shell date -u +%Y-%m-%d)

START ?= 2026-01-01
END ?= 2026-01-07
DAYS ?= 7
DEVICES ?= 500
TOPIC := telemetry.raw

.PHONY: help up down clean build topic simulate live live-stop backfill idempotency-demo stats \
        dbt-docs test lint fixture ci-dbt ps logs

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

# The UI port from .env (compose reads it there too), default 8080
AIRFLOW_PORT ?= $(or $(shell sed -n 's/^AIRFLOW_PORT=//p' .env 2>/dev/null),8080)

up: build ## build images, start Kafka + Airflow, create the topic, install dbt packages and seeds
	mkdir -p data
	$(COMPOSE) up -d --wait kafka airflow-postgres airflow-scheduler airflow-webserver
	$(MAKE) topic
	@echo "Airflow UI: http://localhost:$(AIRFLOW_PORT) (admin / admin)"

build: ## build the pipeline image
	$(COMPOSE) build airflow-scheduler

topic: ## create the Kafka topic (6 partitions, infinite retention for replayable history)
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
	  --create --if-not-exists --topic $(TOPIC) --partitions 6 --replication-factor 1 --config retention.ms=-1

down: ## stop everything (data and volumes kept)
	$(COMPOSE) --profile live down

clean: ## stop everything and wipe ./data, the Kafka log and the Airflow DB
	$(COMPOSE) --profile live down -v --remove-orphans
	rm -rf data

simulate: ## publish historical telemetry: make simulate DAYS=7 DEVICES=500 START=2026-01-01
	$(EXEC) python -m simulator.produce --mode historical --devices $(DEVICES) --start $(START) --days $(DAYS)

live: ## start the live simulator and unpause the hourly DAG
	$(COMPOSE) --profile live up -d simulator
	$(EXEC) airflow dags unpause telemetry_pipeline

live-stop: ## stop the live simulator and pause the hourly DAG
	$(EXEC) airflow dags pause telemetry_pipeline
	$(COMPOSE) --profile live stop simulator

backfill: ## idempotent backfill: make backfill START=2026-01-01 END=2026-01-07 (timed + fingerprinted)
	python3 scripts/measure_resources.py -- $(EXEC) python scripts/backfill.py --start $(START) --end $(END)

idempotency-demo: ## run the same backfill twice and compare row counts + checksums
	$(MAKE) backfill
	$(MAKE) backfill
	$(EXEC) cat /opt/data/stats/idempotency.json

stats: ## collect stats.json and regenerate the README stats table
	$(EXEC) python scripts/stats.py --readme

dbt-docs: ## generate dbt docs (open dbt/target/static_index.html)
	$(EXEC) bash -c "cd dbt && dbt docs generate --static"
	@echo "open dbt/target/static_index.html"

test: ## pytest (inside the image: Java 17 + Spark + Airflow)
	$(EXEC) pytest -q

lint: ## ruff
	$(EXEC) ruff check .

fixture: ## tiny Kafka-free dataset in data/fixture (what CI builds)
	$(EXEC) env DATA_DIR=/opt/data/fixture bash scripts/build_fixture.sh

ci-dbt: fixture ## dbt parse + build + freshness + docs against the fixture
	$(EXEC) env DATA_DIR=/opt/data/fixture bash -c "cd dbt && dbt deps --quiet && dbt parse && dbt seed \
	  && for d in 2025-12-31 2026-01-01 2026-01-02 2026-01-03; do dbt snapshot --vars \"{run_date: \$$d}\"; done \
	  && dbt build && dbt source freshness && dbt docs generate"

ps: ## container status
	$(COMPOSE) ps

logs: ## follow scheduler logs
	$(COMPOSE) logs -f airflow-scheduler
