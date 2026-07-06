"""
transformations/layer2/dim_product_l2.py

Layer 2 (GDO report-ready) for DIM_PRODUCT. Reads curated, adds one
derived business column (category path) as an example of finalization
logic, and writes gold_load_timestamp for lineage.
"""

from pyspark.sql.functions import col, concat_ws, current_timestamp


def run(spark, metadata_row):
    curated_table = f"{metadata_row['layer1_target_catalog']}.{metadata_row['layer1_target_schema']}.{metadata_row['layer1_target_table']}"
    df = spark.table(curated_table)

    df = (
        df
        .withColumn(
            "product_category_path",
            concat_ws(" > ", col("product_group"), col("sub_group"),
                      col("sub_sub_group"), col("sub_sub_sub_group"))
        )
        .withColumn("gold_load_timestamp", current_timestamp())
    )

    return df
