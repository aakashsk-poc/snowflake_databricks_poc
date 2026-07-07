"""Layer 2 (GDO report-ready) for DIM_CUSTOMER. Adds hierarchy path."""
from pyspark.sql.functions import col, concat_ws, current_timestamp, coalesce, lit

def run(spark, metadata_row):
    curated_table = f"{metadata_row['layer1_target_catalog']}.{metadata_row['layer1_target_schema']}.{metadata_row['layer1_target_table']}"
    df = spark.table(curated_table)
    df = (
        df.withColumn(
            "customer_hierarchy_path",
            concat_ws(" > ", coalesce(col("parent_name"), lit("")), col("customer_name"))
        )
        .withColumn("gold_load_timestamp", current_timestamp())
    )
    return df
