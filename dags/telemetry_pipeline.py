"""Telemetry pipeline: Kafka -> bronze -> silver -> dbt (snapshot + star schema + tests) -> stats.

Two DAGs share one task chain:

* ``telemetry_pipeline`` - ``@hourly`` for the live stream (catchup on, one run at a time).
  Each run rebuilds silver for yesterday *and* today, so events that arrive after midnight
  still reach the day they belong to.
* ``telemetry_backfill`` - ``@daily``, never scheduled by itself; ``make backfill`` drives it
  with ``airflow dags backfill``. One run per event date instead of 24 identical hourly runs.

Every task is parameterised by the logical date ``{{ ds }}`` and is idempotent: re-running
a date replaces that date's silver partition, rebuilds the dbt tables from silver and
leaves the snapshot unchanged, so a second backfill gives identical output.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = os.environ.get("PROJECT_DIR", "/opt/project")
DATA = os.environ.get("DATA_DIR", "/opt/data")
SIM_START = os.environ.get("SIM_START_DATE", "2026-01-01")  # first simulated day (backfill DAG start)
LIVE_START = os.environ.get("LIVE_START_DATE") or SIM_START  # set by `make up` to the day the stack started

SPARK_SUBMIT = (
    "spark-submit --master local[2] --driver-memory ${SPARK_DRIVER_MEMORY:-2g} "
    "--conf spark.ui.enabled=false"
)
DBT = f"cd {PROJECT}/dbt && ([ -d dbt_packages/dbt_utils ] || dbt deps --quiet) && dbt"
DBT_VARS = "--vars '{run_date: \"{{ ds }}\"}'"
TASK_ORDER = ["bronze_ingest", "silver_transform", "dbt_source_freshness", "dbt_snapshot", "dbt_build", "publish_stats"]

log = logging.getLogger(__name__)


def notify_failure(context) -> None:
    """Log one clear line per failure and, if ALERT_WEBHOOK_URL is set, post it as JSON."""
    ti = context["task_instance"]
    summary = {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context["run_id"],
        "logical_date": context["ds"],
        "try_number": ti.try_number,
        "error": str(context.get("exception"))[:500],
        "log_url": ti.log_url,
    }
    log.error("TASK FAILED %s", json.dumps(summary))
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if url:
        if not url.startswith(("http://", "https://")):
            log.warning("ALERT_WEBHOOK_URL must be http(s); not sending")
            return
        body = json.dumps({"text": f"Pipeline failure: {summary}"}).encode()
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")  # noqa: S310
        try:
            urllib.request.urlopen(req, timeout=10)  # noqa: S310 - scheme checked above
        except OSError as exc:
            log.warning("alert webhook failed: %s", exc)


def build_dag(dag_id: str, schedule: str, start: str, catchup: bool, silver_dates: str, doc: str) -> DAG:
    default_args = {
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notify_failure,
    }
    with DAG(
        dag_id=dag_id,
        schedule=schedule,
        start_date=pendulum.parse(start, tz="UTC"),
        catchup=catchup,
        max_active_runs=1,
        default_args=default_args,
        doc_md=doc,
        tags=["telemetry", "spark", "dbt"],
        render_template_as_native_obj=False,
    ) as dag:
        bronze = BashOperator(
            task_id="bronze_ingest",
            bash_command=f"{SPARK_SUBMIT} {PROJECT}/spark_jobs/bronze_ingest.py --source ${{BRONZE_SOURCE:-kafka}}",
            execution_timeout=timedelta(minutes=30),
        )
        silver = BashOperator(
            task_id="silver_transform",
            bash_command=f"{SPARK_SUBMIT} {PROJECT}/spark_jobs/silver_transform.py {silver_dates}",
            execution_timeout=timedelta(minutes=30),
        )
        freshness = BashOperator(
            task_id="dbt_source_freshness",
            # A backfill re-processes old loads on purpose, so a stale source is expected there.
            bash_command=(
                f"{DBT} source freshness"
                "{% if dag_run.run_type == 'backfill' %}"
                " || echo 'freshness breach ignored for a backfill run'"
                "{% endif %}"
            ),
            execution_timeout=timedelta(minutes=10),
        )
        snapshot = BashOperator(
            task_id="dbt_snapshot",
            # "as of the day before" first: a no-op except on the very first run, where it
            # records every device's factory firmware before any upgrade (see the snapshot).
            bash_command=(
                f"{DBT} snapshot --vars '{{run_date: \"{{{{ macros.ds_add(ds, -1) }}}}\"}}' "
                f"&& dbt snapshot {DBT_VARS}"
            ),
            execution_timeout=timedelta(minutes=15),
        )
        build = BashOperator(
            task_id="dbt_build",
            # models + tests + snapshots in DAG order; an error-severity test fails the task
            bash_command=(
                f"{DBT} build {DBT_VARS} && mkdir -p {DATA}/stats "
                f"&& cp target/run_results.json {DATA}/stats/dbt_build_results.json "
                f"&& cp target/manifest.json {DATA}/stats/dbt_manifest.json"
            ),
            execution_timeout=timedelta(minutes=30),
        )
        stats = BashOperator(
            task_id="publish_stats",
            bash_command=f"python {PROJECT}/scripts/stats.py --date {{{{ ds }}}}",
            execution_timeout=timedelta(minutes=10),
        )
        bronze >> silver >> freshness >> snapshot >> build >> stats
    return dag


live_dag = build_dag(
    dag_id="telemetry_pipeline",
    schedule="@hourly",
    start=LIVE_START,
    catchup=True,
    silver_dates="--date {{ macros.ds_add(ds, -1) }} --date {{ ds }}",
    doc=__doc__,
)

backfill_dag = build_dag(
    dag_id="telemetry_backfill",
    schedule="@daily",
    start=SIM_START,
    catchup=False,
    silver_dates="--date {{ ds }}",
    doc=__doc__,
)
