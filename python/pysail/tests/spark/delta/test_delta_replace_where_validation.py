import pytest
from pyspark.sql.types import Row


def _rows(spark, target):
    return spark.read.format("delta").load(str(target)).orderBy("id").collect()


def test_v1_replace_where_rejects_false_and_preserves_table(spark, tmp_path):
    path = tmp_path / "delta_replace_where_v1_validation"
    spark.createDataFrame([Row(id=1, category="A"), Row(id=2, category="B")]).coalesce(1).write.format("delta").save(
        str(path)
    )

    bad = spark.createDataFrame([Row(id=3, category="C")])
    with pytest.raises(Exception, match="DELTA_REPLACE_WHERE_MISMATCH"):
        bad.write.format("delta").mode("overwrite").option("replaceWhere", "category = 'A'").save(str(path))

    assert _rows(spark, path) == [Row(id=1, category="A"), Row(id=2, category="B")]


def test_v1_replace_where_rejects_null_and_preserves_table(spark, tmp_path):
    path = tmp_path / "delta_replace_where_v1_null_validation"
    spark.createDataFrame([Row(id=1, category="A"), Row(id=2, category="B")]).coalesce(1).write.format("delta").save(
        str(path)
    )

    bad = spark.createDataFrame([(3, None)], "id BIGINT, category STRING")
    with pytest.raises(Exception, match="DELTA_REPLACE_WHERE_MISMATCH"):
        bad.write.format("delta").mode("overwrite").option("replaceWhere", "category = 'A'").save(str(path))

    assert _rows(spark, path) == [Row(id=1, category="A"), Row(id=2, category="B")]


def test_v1_replace_where_internal_marker_does_not_leak_to_delta_log(spark, tmp_path):
    path = tmp_path / "delta_replace_where_marker"
    spark.createDataFrame([Row(id=1, category="A"), Row(id=2, category="B")]).write.format("delta").save(str(path))

    (
        spark.createDataFrame([Row(id=3, category="A")])
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", "category = 'A'")
        .save(str(path))
    )

    assert _rows(spark, path) == [Row(id=2, category="B"), Row(id=3, category="A")]
    delta_log = path / "_delta_log"
    log_text = "\n".join(entry.read_text(encoding="utf-8") for entry in sorted(delta_log.glob("*.json")))
    assert "__sail_v1_replace_where__:" not in log_text
