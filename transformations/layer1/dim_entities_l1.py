"""Layer 1 (curated) for DIM_ENTITIES. Snake_case rename, trim, cast, dedup."""
from pyspark.sql.functions import col, trim, current_timestamp, row_number
from pyspark.sql.window import Window

def run(spark, metadata_row):
    raw_table = f"{metadata_row['layer0_target_catalog']}.{metadata_row['layer0_target_schema']}.{metadata_row['layer0_target_table']}"
    df = spark.table(raw_table)

    df = (
        df.select(
            trim(col("ENTITY_CODE")).alias("entity_code"),
            trim(col("ENTITY_NAME")).alias("entity_name"),
            trim(col("ENTITY_GROUP")).alias("entity_group"),
            trim(col("ENTITY_GROUP_SHORT_NAME")).alias("entity_group_short_name"),
            trim(col("DEFAULT_MARKET")).alias("default_market"),
            trim(col("COMPANY")).alias("company"),
            col("DWH_UPDATE_DATE").cast("timestamp").alias("dwh_update_date"),
        )
        .withColumn("curated_load_timestamp", current_timestamp())
        .withColumn("job_run_id", col("entity_code"))
    )

    window = Window.partitionBy("entity_code").orderBy(col("curated_load_timestamp").desc())
    df = df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")
    return df
