def test_sail_save_as_table_replace_where_preserves_rows_in_hms(
    jvm_spark,
    spark,
    hms_s3_database,
):
    table_fqn = f"{hms_s3_database}.replace_where_roundtrip"

    jvm_spark.sql(
        f"""
        CREATE TABLE {table_fqn}
        USING DELTA
        AS SELECT 1 AS id, 'A' AS category
        UNION ALL
        SELECT 2 AS id, 'B' AS category
        """
    )

    replacement = spark.createDataFrame([(3, "A")], ["id", "category"])
    (
        replacement.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", "category = 'A'")
        .saveAsTable(table_fqn)
    )

    sail_rows = [tuple(row) for row in spark.sql(f"SELECT id, category FROM {table_fqn} ORDER BY id").collect()]
    assert sail_rows == [(2, "B"), (3, "A")]

    jvm_spark.catalog.refreshTable(table_fqn)
    spark_rows = [tuple(row) for row in jvm_spark.sql(f"SELECT id, category FROM {table_fqn} ORDER BY id").collect()]
    assert spark_rows == [(2, "B"), (3, "A")]
