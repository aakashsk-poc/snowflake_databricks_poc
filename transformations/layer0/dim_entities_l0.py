"""
transformations/layer0/dim_entities_l0.py

Layer 0 (raw landing) for DIM_PRODUCT. Reads the source file from a Unity
Catalog Volume -- this is the ONLY thing that changes when you move to the
real project: swap this read for a Snowflake JDBC read. Everything below
the read (lineage columns, return shape) stays identical.
"""

from pyspark.sql.functions import current_timestamp, lit, input_file_name
from pyspark.sql.functions import col

def run(spark, metadata_row):
    """
    metadata_row.source_table holds the full Volume path for this POC
    (set in the metadata INSERT statement). In production this would
    instead be the Snowflake table name, and the read below would be
    a Snowflake JDBC read instead of spark.read.csv().
    """
    source_path = metadata_row["source_table"]  # e.g. /Volumes/.../dim_product_sample.csv change while we snowflake.

    df = (
        spark.read
        .option("header", "true")
        .csv(source_path)
    )


    df = (
        df
        .withColumn("source_file_name", col("_metadata.file_path")) # need to change for snowflake
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("job_run_id", lit(metadata_row["table_id"]))
    )


    return df
