import pytest

from pyspark.sql.types import Row


_ERROR = "unsupported Delta option affects data correctness"


def test_rejects_idempotent_transaction_options_instead_of_writing_duplicates(spark, tmp_path):
    table_path = tmp_path / "delta_txn_options"

    with pytest.raises(Exception, match=_ERROR):
        (
            spark.range(3)
            .write.format("delta")
            .option("txnAppId", "retry-test")
            .option("txnVersion", "1")
            .save(str(table_path))
        )


def test_rejects_dynamic_partition_overwrite_instead_of_deleting_untouched_partitions(spark, tmp_path):
    table_path = tmp_path / "delta_dynamic_partition_overwrite"
    spark.createDataFrame([Row(id=1, category="A"), Row(id=2, category="B")]).write.format("delta").partitionBy(
        "category"
    ).save(str(table_path))

    replacement = spark.createDataFrame([Row(id=3, category="A")])
    with pytest.raises(Exception, match=_ERROR):
        (
            replacement.write.format("delta")
            .mode("overwrite")
            .partitionBy("category")
            .option("partitionOverwriteMode", "dynamic")
            .save(str(table_path))
        )

    assert spark.read.format("delta").load(str(table_path)).orderBy("id").collect() == [
        Row(id=1, category="A"),
        Row(id=2, category="B"),
    ]


def test_session_dynamic_partition_overwrite_is_rejected_at_write_and_writer_static_overrides(
    spark, tmp_path
):
    original = spark.conf.get("spark.sql.sources.partitionOverwriteMode")
    table_path = tmp_path / "delta_dynamic_partition_overwrite_session"
    spark.createDataFrame([Row(id=1, category="A"), Row(id=2, category="B")]).write.format("delta").partitionBy(
        "category"
    ).save(str(table_path))

    try:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        replacement = spark.createDataFrame([Row(id=3, category="A")])

        with pytest.raises(Exception, match=_ERROR):
            (
                replacement.write.format("delta")
                .mode("overwrite")
                .partitionBy("category")
                .save(str(table_path))
            )

        assert spark.read.format("delta").load(str(table_path)).orderBy("id").collect() == [
            Row(id=1, category="A"),
            Row(id=2, category="B"),
        ]

        (
            replacement.write.format("delta")
            .mode("overwrite")
            .partitionBy("category")
            .option("partitionOverwriteMode", "static")
            .save(str(table_path))
        )
        assert spark.read.format("delta").load(str(table_path)).collect() == [
            Row(id=3, category="A")
        ]
    finally:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", original)


def test_safe_explicit_defaults_are_accepted(spark, tmp_path):
    table_path = tmp_path / "delta_safe_semantic_defaults"
    spark.createDataFrame([Row(id=1, category="A")]).write.format("delta").partitionBy("category").save(str(table_path))

    (
        spark.createDataFrame([Row(id=2, category="A")])
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("category")
        .option("partitionOverwriteMode", "static")
        .option("dataChange", "true")
        .save(str(table_path))
    )
    assert spark.read.format("delta").load(str(table_path)).collect() == [Row(id=2, category="A")]

    assert (
        spark.read.format("delta")
        .option("readChangeFeed", "false")
        .load(str(table_path))
        .collect()
        == [Row(id=2, category="A")]
    )


def test_rejects_change_data_feed_options_instead_of_returning_a_snapshot(spark, tmp_path):
    table_path = tmp_path / "delta_cdf_options"
    spark.range(2).write.format("delta").save(str(table_path))

    with pytest.raises(Exception, match=_ERROR):
        (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "0")
            .load(str(table_path))
            .collect()
        )


def test_rejects_invalid_partition_overwrite_mode(spark, tmp_path):
    table_path = tmp_path / "delta_invalid_partition_overwrite_mode"

    with pytest.raises(Exception, match=r"invalid option.*partitionOverwriteMode.*bogus"):
        (
            spark.range(1)
            .write.format("delta")
            .mode("overwrite")
            .option("partitionOverwriteMode", "bogus")
            .save(str(table_path))
        )
