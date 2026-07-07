"""Layer 2 (GDO report-ready) for DIM_ENTITIES. Pass-through, adds lineage."""
from pyspark.sql.functions import current_timestamp

def run(spark, metadata_row):
    curated_table = f"{metadata_row['layer1_target_catalog']}.{metadata_row['layer1_target_schema']}.{metadata_row['layer1_target_table']}"
    df = spark.table(curated_table)
    df = df.withColumn("gold_load_timestamp", current_timestamp())
    return df
