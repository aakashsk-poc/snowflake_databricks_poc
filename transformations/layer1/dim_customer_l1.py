"""Layer 1 (curated) for DIM_CUSTOMER. Snake_case rename, trim, cast, dedup."""
from pyspark.sql.functions import col, trim, current_timestamp, row_number
from pyspark.sql.window import Window

def run(spark, metadata_row):
    raw_table = f"{metadata_row['layer0_target_catalog']}.{metadata_row['layer0_target_schema']}.{metadata_row['layer0_target_table']}"
    df = spark.table(raw_table)

    df = (
        df.select(
            trim(col("CUSTOMER_ID")).alias("customer_id"),
            trim(col("CUSTOMER_NAME")).alias("customer_name"),
            trim(col("CUSTOMER_GROUP")).alias("customer_group"),
            trim(col("CUSTOMER_GROUP2")).alias("customer_group2"),
            trim(col("CUSTOMER_GROUP3")).alias("customer_group3"),
            trim(col("CUSTOMERPARENTID")).alias("customer_parent_id"),
            trim(col("PARENTNAME")).alias("parent_name"),
            trim(col("PATHNAME")).alias("path_name"),
            col("DEPTH").cast("bigint").alias("depth"),
            trim(col("BT_CITY")).alias("bt_city"),
            trim(col("BT_PROVINCE")).alias("bt_province"),
            trim(col("PROVINCE")).alias("province"),
            trim(col("COUNTRY")).alias("country"),
        )
        .withColumn("curated_load_timestamp", current_timestamp())
        .withColumn("job_run_id", col("customer_id"))
    )

    window = Window.partitionBy("customer_id").orderBy(col("curated_load_timestamp").desc())
    df = df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")
    return df
