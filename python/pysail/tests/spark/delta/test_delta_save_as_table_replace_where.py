from pyspark.sql.types import Row


def test_save_as_table_replace_where_preserves_unmatched_rows(spark, tmp_path):
    table_path = tmp_path / "delta_save_as_table_replace_where"
    table_name = "delta_save_as_table_replace_where_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"CREATE TABLE {table_name} (id BIGINT, category STRING) USING DELTA LOCATION '{table_path}'"
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'A'), (2, 'B')")  # noqa: S608

        replacement = spark.createDataFrame([Row(id=3, category="A")])
        (
            replacement.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", "category = 'A'")
            .saveAsTable(table_name)
        )

        assert spark.table(table_name).orderBy("id").collect() == [
            Row(id=2, category="B"),
            Row(id=3, category="A"),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_save_as_table_replace_where_creates_absent_target(spark):
    table_name = "delta_save_as_table_replace_where_create_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        replacement = spark.createDataFrame([Row(id=1, category="A")])
        (
            replacement.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", "category = 'A'")
            .saveAsTable(table_name)
        )

        assert spark.table(table_name).collect() == [Row(id=1, category="A")]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
