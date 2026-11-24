import json

import pytest

pytest.importorskip("duckdb")
from scripts import stats  # noqa: E402


def test_committed_files_reads_the_spark_metadata_log(tmp_path):
    table = tmp_path / "t"
    (table / "_spark_metadata").mkdir(parents=True)
    a, b = table / "d=1" / "a.parquet", table / "d=1" / "b.parquet"
    (table / "d=1").mkdir()
    a.write_bytes(b"x")
    b.write_bytes(b"y")
    (table / "d=1" / "orphan.parquet").write_bytes(b"z")  # left by a failed batch, never committed
    (table / "_spark_metadata" / "0").write_text("v1\n" + json.dumps({"path": f"file://{a}", "action": "add"}))
    (table / "_spark_metadata" / "1").write_text("v1\n" + json.dumps({"path": f"file://{b}", "action": "add"}))
    assert stats.committed_files(table) == [a, b]


def test_compare_reports_each_difference():
    a = {"lake": {"silver/heartbeat": {"rows": 3}}, "warehouse": {"marts.x": {"rows": 1, "md5": "a"}}}
    b = {"lake": {"silver/heartbeat": {"rows": 3}}, "warehouse": {"marts.x": {"rows": 1, "md5": "b"}}}
    assert stats.compare(a, a) == []
    assert stats.compare(a, b) == ["warehouse:marts.x"]


def test_update_readme_rewrites_only_the_marked_block(tmp_path):
    readme = tmp_path / "README.md"
    readme.write_text(f"intro\n{stats.START}\nold\n{stats.END}\noutro\n")
    assert stats.update_readme("| new |", readme)
    assert readme.read_text() == f"intro\n{stats.START}\n| new |\n{stats.END}\noutro\n"
