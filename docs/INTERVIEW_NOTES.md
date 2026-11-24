# Interview notes

Short answers to the questions this project invites, tied to the code.

## Why bronze / silver / gold?

Each layer has one job and can be rebuilt from the one before it.

- **Bronze** is the raw record exactly as it arrived (JSON string + Kafka metadata). If
  silver logic has a bug, fix it and re-run silver from bronze; nothing is lost.
- **Silver** is clean, typed, deduplicated events, one folder per event type. It is the
  contract for everything downstream.
- **Gold** is modelled for questions: a star schema with tested business definitions.

## What exactly makes a backfill idempotent here?

Every step either reads nothing new or replaces exactly what it owns:

1. Bronze: Kafka offsets are in the Structured Streaming checkpoint and the Parquet sink
   commits files in `_spark_metadata`, so a re-run reads nothing twice.
2. Silver: dynamic partition overwrite replaces only `event_date=D`, and rows are written
   in a total order, so the data pages are byte-identical (`tests/test_silver.py`).
   Gotcha found on the way: parquet-mr writes each column's list of encodings from a Java
   hash set, so that footer field's order changes between JVM runs; the checksum sorts it.
3. dbt: models are full rebuilds from silver filtered to `run_date`; the snapshot's
   `updated_at` comes from the data, so re-running an old date finds nothing newer.
   Aggregates use DECIMAL, because a parallel floating-point `avg()` can differ in the last
   bit from run to run (the first idempotency check caught this in `fct_device_daily`).

`make idempotency-demo` proves it: two backfills, then SHA-256 of every Parquet file and a
hash of every warehouse table are compared.

## `availableNow` vs a batch read with explicit offsets?

A batch read (`spark.read.format("kafka")` with `startingOffsets`/`endingOffsets`) makes
*you* store and pass offsets between runs. `trigger(availableNow=True)` keeps them in the
checkpoint: each run processes everything up to the offsets that existed when it started
(in several micro-batches if `maxOffsetsPerTrigger` says so), commits, and stops. You get
streaming's exactly-once bookkeeping with batch scheduling, which fits an Airflow task.

## How are late events handled? What does the D+1 window cost?

Bronze is partitioned by arrival time. The silver job for date D reads bronze D and D+1
and keeps rows whose `event_ts` falls on D, so an event from 23:50 that arrives at 02:00
is recovered. Costs: D is only final after D+1 has landed (the hourly DAG therefore
rebuilds yesterday and today), each run reads two days of bronze, and anything later
than the window is dropped (counted as `too_late_dropped` in the run report).

## Why an SCD2 device dimension? How do facts join "as of" event time?

Firmware changes over time, and questions like "crash rate before vs after 1.1.0" need
the firmware that was live *when* the event happened, not today's. `dim_device` has one
row per device per firmware window; facts join with
`event_ts >= valid_from and event_ts < valid_to` and store the `device_key` of that window.

## What changes at 100x volume?

More Kafka partitions (and consumers); a Spark cluster instead of `local[2]`; object
storage with a table format (Iceberg/Delta: ACID, compaction, time travel) instead of
plain Parquet folders; a warehouse such as Snowflake/BigQuery; incremental dbt models
keyed on the run date instead of full rebuilds; silver writing several files per partition.

## Why Parquet partitioned by date? When does over-partitioning hurt?

Columnar storage reads only the columns a query needs; date partitions let both Spark and
DuckDB skip whole folders (`where event_date = …`) and give the overwrite unit for
idempotent re-runs. Over-partitioning (for example by device or by minute) creates many tiny
files: listing and opening files dominates, compression gets worse, and the metadata grows.
Bronze adds `event_hour` because it is written every hour; silver stays at day level.
