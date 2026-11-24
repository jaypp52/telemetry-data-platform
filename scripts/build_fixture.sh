#!/usr/bin/env bash
# Tiny end-to-end dataset without Kafka: simulator (jsonl) -> bronze (file stream) -> silver.
# Used by CI before `dbt build`, and by `make fixture`. Fault rates are raised so every
# data-quality path (dedupe, late, clock, schema, quarantine) is exercised.
set -euo pipefail
cd "$(dirname "$0")/.."
export DATA_DIR="${DATA_DIR:-$PWD/data/fixture}"
DEVICES="${DEVICES:-25}"
START="${START:-2026-01-01}"
DAYS="${DAYS:-3}"

rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"
python -m simulator.produce --mode historical --sink jsonl --devices "$DEVICES" --start "$START" --days "$DAYS" \
  --duplicate-rate 0.02 --late-rate 0.03 --clock-error-rate 0.01 --malformed-rate 0.002 --crash-multiplier 4
python spark_jobs/bronze_ingest.py --source files
dates=()
for i in $(seq 0 $((DAYS - 1))); do dates+=(--date "$(date -u -d "$START + $i day" +%F)"); done
python spark_jobs/silver_transform.py "${dates[@]}"
echo "fixture ready in $DATA_DIR"
