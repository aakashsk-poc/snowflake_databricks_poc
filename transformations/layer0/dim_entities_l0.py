"""Layer 0 (raw landing) for DIM_ENTITIES. Same shape as dim_product_l0.py."""
from pyspark.sql.functions import current_timestamp, lit, input_file_name

def run(spark, metadata_row):
    source_path = metadata_row["source_table"]
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(source_path)
    df = (
        df.withColumn("source_file_name", input_file_name())
          .withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("job_run_id", lit(metadata_row["table_id"]))
    )
    return df
