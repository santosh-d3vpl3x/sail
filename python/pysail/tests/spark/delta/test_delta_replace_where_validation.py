import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import Row


def _rows(spark, target):
    return spark.read.format("delta").load(str(target)).orderBy("id").collect()


def test_v1_replace_where_rejects_false_and_preserves_table(spark, tmp_path):
    path = tmp_path / "delta_replace_where_v1_validation"
    spark.createDataFrame([Row(id=1, category="A"), Row(id=2, category="B")]).coalesce(1).write.format(
        "delta"
    ).save(str(path))

    bad = spark.createDataFrame([Row(id=3, category="C")])
    with pytest.raises(Exception, match="DELTA_REPLACE_WHERE_MISMATCH"):
        bad.write.format("delta").mode("overwrite").option("replaceWhere", "category = 'A'").save(str(path))

    assert _rows(spark, path) == [Row(id=1, category="A"), Row(id=2, category="B")]


def test_v1_replace_where_validates_after_target_cast(spark, tmp_path):
    path = tmp_path / "delta_replace_where_cast_validation"
    spark.createDataFrame([Row(id=2)]).write.format("delta").save(str(path))

    # The target is BIGINT. 1.5 satisfies `id > 1` as DOUBLE but is stored as BIGINT 1,
    # which must fail the replacement predicate after target conversion.
    bad = spark.createDataFrame([(1.5,)], ["id"])
    with pytest.raises(Exception, match="DELTA_REPLACE_WHERE_MISMATCH"):
        bad.write.format("delta").mode("overwrite").option("replaceWhere", "id > 1").save(str(path))

    assert _rows(spark, path) == [Row(id=2)]


def test_sql_replace_where_rejects_mismatching_input(spark, tmp_path):
    path = tmp_path / "delta_replace_where_sql_validation"
    table = "delta_replace_where_sql_validation_test"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    try:
        spark.sql(f"CREATE TABLE {table} (id BIGINT, category STRING) USING DELTA LOCATION '{path}'")
        spark.sql(f"INSERT INTO {table} VALUES (1, 'A'), (2, 'B')")

        with pytest.raises(Exception, match="DELTA_REPLACE_WHERE_MISMATCH"):
            spark.sql(
                f"INSERT INTO {table} REPLACE WHERE category = 'A' "
                "SELECT * FROM VALUES (3, 'C') AS t(id, category)"
            )

        assert spark.table(table).orderBy("id").collect() == [
            Row(id=1, category="A"),
            Row(id=2, category="B"),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_v2_overwrite_rejects_mismatching_input(spark, tmp_path):
    path = tmp_path / "delta_replace_where_v2_validation"
    table = "delta_replace_where_v2_validation_test"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    try:
        spark.sql(f"CREATE TABLE {table} (id BIGINT, category STRING) USING DELTA LOCATION '{path}'")
        spark.sql(f"INSERT INTO {table} VALUES (1, 'A'), (2, 'B')")

        bad = spark.createDataFrame([Row(id=3, category="C")])
        with pytest.raises(Exception, match="DELTA_REPLACE_WHERE_MISMATCH"):
            bad.writeTo(table).overwrite(F.expr("category = 'A'"))

        assert spark.table(table).orderBy("id").collect() == [
            Row(id=1, category="A"),
            Row(id=2, category="B"),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
