import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
    os.environ.setdefault("SPARK_MASTER", "local[2]")
    os.environ.setdefault("SPARK_DRIVER_MEMORY", "1g")
    from spark_jobs.common import get_spark

    session = get_spark("pytest", kafka=bool(os.environ.get("KAFKA_BOOTSTRAP_SERVERS")))
    yield session
    session.stop()


@pytest.fixture()
def lake(tmp_path):
    from spark_jobs.common import Paths

    return Paths(tmp_path / "data")
