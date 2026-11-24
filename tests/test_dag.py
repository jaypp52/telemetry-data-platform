from datetime import timedelta
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "dags"
ORDER = ["bronze_ingest", "silver_transform", "dbt_source_freshness", "dbt_snapshot", "dbt_build", "publish_stats"]


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_dags_import_without_errors(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {"telemetry_pipeline", "telemetry_backfill"}


@pytest.mark.parametrize("dag_id", ["telemetry_pipeline", "telemetry_backfill"])
def test_task_order_and_retry_policy(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert [t.task_id for t in dag.topological_sort()] == ORDER
    for upstream, downstream in zip(ORDER, ORDER[1:], strict=False):
        assert dag.get_task(downstream).upstream_task_ids == {upstream}
    for task in dag.tasks:
        assert task.retries == 2
        assert task.retry_delay == timedelta(minutes=2)
        assert task.execution_timeout is not None
        assert task.on_failure_callback is not None
    assert dag.max_active_runs == 1


def test_schedules(dagbag):
    live, backfill = dagbag.get_dag("telemetry_pipeline"), dagbag.get_dag("telemetry_backfill")
    assert live.schedule_interval == "@hourly" and live.catchup
    assert backfill.schedule_interval == "@daily" and not backfill.catchup


def test_commands_are_parameterised_by_logical_date(dagbag):
    live = dagbag.get_dag("telemetry_pipeline")
    assert "--date {{ macros.ds_add(ds, -1) }} --date {{ ds }}" in live.get_task("silver_transform").bash_command
    backfill = dagbag.get_dag("telemetry_backfill")
    assert backfill.get_task("silver_transform").bash_command.endswith("--date {{ ds }}")
    assert "run_date" in backfill.get_task("dbt_build").bash_command
    assert "dbt build" in backfill.get_task("dbt_build").bash_command
    assert "dbt source freshness" in backfill.get_task("dbt_source_freshness").bash_command
    assert "--date {{ ds }}" in backfill.get_task("publish_stats").bash_command


def test_failure_callback_logs_summary(dagbag, caplog, monkeypatch):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    callback = dagbag.get_dag("telemetry_backfill").get_task("dbt_build").on_failure_callback

    class TI:
        dag_id, task_id, try_number, log_url = "telemetry_backfill", "dbt_build", 3, "http://x/log"

    callback({"task_instance": TI(), "run_id": "backfill__2026-01-02", "ds": "2026-01-02",
              "exception": ValueError("boom")})
    assert "TASK FAILED" in caplog.text and "dbt_build" in caplog.text and "boom" in caplog.text
