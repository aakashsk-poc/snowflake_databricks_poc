"""
transformations/layer1/dim_product_l1.py

Layer 1 (curated) for DIM_PRODUCT. Placeholder STTM logic: rename to
snake_case, trim strings, cast types, dedup on product_id. When the real
STTM arrives, only the column_mapping / cast rules below change -- the
orchestrator and audit framework never need to be touched.
"""

from pyspark.sql.functions import (
    col, trim, current_timestamp, row_number, upper, when
)
from pyspark.sql.window import Window


def run(spark, metadata_row):
    raw_table = f"{metadata_row['layer0_target_catalog']}.{metadata_row['layer0_target_schema']}.{metadata_row['layer0_target_table']}"
    df = spark.table(raw_table)

    df = (
        df
        .select(
            trim(col("PRODUCT_ID")).alias("product_id"),
            trim(col("DESCRIPTION")).alias("description"),
            trim(col("PRODUCT_GROUP")).alias("product_group"),
            trim(col("SUB_GROUP")).alias("sub_group"),
            trim(col("SUB_SUB_GROUP")).alias("sub_sub_group"),
            trim(col("SUB_SUB_SUB_GROUP")).alias("sub_sub_sub_group"),
            trim(col("COLORFLAVOR")).alias("color_flavor"),
            trim(col("FLAVOR_FAMILY")).alias("flavor_family"),
            col("PACK").cast("decimal(18,5)").alias("pack"),
            trim(col("CONFIGURATION")).alias("configuration"),
            trim(col("BRAND")).alias("brand"),
            trim(col("EDITION")).alias("edition"),
            trim(col("SCREW")).alias("screw"),
            when(col("CYLINDER_QTY") == "", None)
                .otherwise(col("CYLINDER_QTY")).cast("int").alias("cylinder_qty"),
            (upper(trim(col("GAS_EXCHANGE"))) == "Y").alias("gas_exchange"),
        )
        .withColumn("curated_load_timestamp", current_timestamp())
        .withColumn("job_run_id", col("product_id"))  # placeholder, real job_run_id wired via widget in production
    )

    # Dedup: keep the most recently ingested row per product_id
    window = Window.partitionBy("product_id").orderBy(col("curated_load_timestamp").desc())
    df = (
        df.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    return df
