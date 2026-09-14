from pyspark.sql.types import Row


def test_replace_where_preserves_null_rows_in_rewritten_file(spark, tmp_path):
    table_path = tmp_path / "delta_replace_where_null_retention"

    seed = spark.createDataFrame(
        [
            Row(id=1, category="A"),
            Row(id=2, category="B"),
            Row(id=3, category=None),
        ],
        "id BIGINT, category STRING",
    ).coalesce(1)
    seed.write.format("delta").save(str(table_path))

    # The bug only manifests when the NULL row shares a physical file with a row
    # selected by replaceWhere. Keep the fixture explicit so file pruning cannot hide it.
    assert len(list(table_path.glob("*.parquet"))) == 1

    replacement = spark.createDataFrame([Row(id=4, category="A")])
    (
        replacement.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", "category = 'A'")
        .save(str(table_path))
    )

    assert spark.read.format("delta").load(str(table_path)).orderBy("id").collect() == [
        Row(id=2, category="B"),
        Row(id=3, category=None),
        Row(id=4, category="A"),
    ]
