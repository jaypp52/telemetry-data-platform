from pyspark.sql import functions as F

from spark_jobs.bronze_ingest import run, to_bronze
from tests.sparkhelpers import heartbeat, record, uid, write_landing


def test_to_bronze_flags_malformed_and_partitions_by_arrival(spark):
    rows = [("k", '{"event_id":"a","event_type":"heartbeat","device_id":"d"}', "t", 0, 1),
            ("k", '{"event_id":"b","event_type":"heart', "t", 0, 2),
            ("k", '{"event_type":"heartbeat"}', "t", 0, 3),
            ("k", "not json at all", "t", 0, 4)]
    schema = "kafka_key string, value string, topic string, kafka_partition int, kafka_offset long"
    df = (spark.createDataFrame(rows, schema)
          .withColumn("kafka_ts", F.to_timestamp(F.lit("2026-01-02 23:30:00"))))
    out = {r["kafka_offset"]: r for r in to_bronze(df).collect()}
    assert [out[i]["is_malformed"] for i in (1, 2, 3, 4)] == [False, True, True, True]
    assert str(out[1]["event_date"]) == "2026-01-02" and out[1]["event_hour"] == 23
    assert out[1]["ingest_ts"] is not None


def test_rerun_never_duplicates_and_picks_up_new_data(spark, lake):
    landing = lake.landing / "telemetry.raw"
    first = [record(heartbeat(uid(i), "2026-01-01T10:00:00Z"), "2026-01-01T10:00:01", i) for i in range(50)]
    first.append(record('{"event_id": "broken', "2026-01-01T10:00:02", 50))
    write_landing(landing, first)

    def counts():
        bronze = spark.read.parquet(str(lake.bronze)).count()
        quarantine = spark.read.parquet(str(lake.quarantine)).count()
        return bronze, quarantine

    run(spark, source="files", p=lake)
    assert counts() == (50, 1)
    run(spark, source="files", p=lake)  # nothing new: the checkpoint makes this a no-op
    assert counts() == (50, 1)

    more = [record(heartbeat(uid(100 + i), "2026-01-02T00:10:00Z"), "2026-01-02T00:10:01", 100 + i) for i in range(7)]
    write_landing(landing, more, name="part-00001.jsonl")
    run(spark, source="files", p=lake)
    assert counts() == (57, 1)
    dates = {str(r["event_date"]) for r in spark.read.parquet(str(lake.bronze)).select("event_date").collect()}
    assert dates == {"2026-01-01", "2026-01-02"}
