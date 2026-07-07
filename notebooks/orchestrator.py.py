# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
"""
notebooks/orchestrator.py

THE generic notebook. Every Workflow task points here, passing table_id and
layer as parameters. This notebook never changes per table -- it looks up
the metadata row, dynamically loads the matching transformation script,
writes the result, and logs the outcome. All per-table business logic
lives in transformations/layerX/*.py, not here.
"""

import sys
import importlib.util

sys.path.append("../framework")
import logger
import write_utils  # noqa: E402

# ---- Task parameters (set by the Workflow task's base_parameters) ----
dbutils.widgets.text("table_id", "")
dbutils.widgets.text("layer", "")
dbutils.widgets.text("catalog", "uc_dev_snt_fdn")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("environment", "dev")

table_id = dbutils.widgets.get("table_id")
layer = dbutils.widgets.get("layer")
catalog = dbutils.widgets.get("catalog")
job_run_id = dbutils.widgets.get("job_run_id") or "manual-run"
environment = dbutils.widgets.get("environment")

workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "unknown")
executed_by = spark.sql("SELECT current_user()").collect()[0][0]

# ---- 1. Look up the metadata row for this table ----
metadata = (
    spark.table(f"{catalog}.config.ingestion_metadata_sample")
    .filter(f"table_id='{table_id}'")
    .collect()
)

if not metadata:
    raise Exception(f"Metadata not found for {table_id}")

metadata_row = metadata[0]
is_active_col = f"layer{layer}_is_active"
script_path_col = f"layer{layer}_script_path"
target_catalog_col = f"layer{layer}_target_catalog"
target_schema_col = f"layer{layer}_target_schema"
target_table_col = f"layer{layer}_target_table"
load_type_col = f"layer{layer}_load_type"
merge_keys_col = f"layer{layer}_merge_keys"

# ---- 2. Skip cleanly if this layer isn't turned on yet ----
if not metadata_row[is_active_col]:
    ctx = logger.start_run(spark, catalog, job_run_id, table_id, layer,
                            metadata_row["source_system"], workspace_url,
                            environment, executed_by,
                            reporting_unit_id=metadata_row["reporting_unit_id"])
    logger.end_run(spark, ctx, status="SKIPPED")
    dbutils.notebook.exit("SKIPPED") 
    #dbutils.notebook.exit("SKIPPED: layer not active")

# ---- 3. Start the audit run ----
run_context = logger.start_run(
    spark, catalog, job_run_id, table_id, layer,
    metadata_row["source_system"], workspace_url, environment, executed_by
    
)
# , reporting_unit_id=metadata_row["reporting_unit_id"],
try:
    # ---- 4. Dynamically load the table's transformation script ----
    script_path = metadata_row[script_path_col]
    spec = importlib.util.spec_from_file_location("transform_module", script_path)
    transform_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(transform_module)

    # Every transformation script exposes a run(spark, metadata_row) function
    result_df = transform_module.run(spark, metadata_row)

    # ---- 5. Write, get back precise read/insert/update counts ----
    write_result = write_utils.write_and_publish(
        spark,
        result_df,
        target_catalog=metadata_row[target_catalog_col],
        target_schema=metadata_row[target_schema_col],
        target_table=metadata_row[target_table_col],
        load_type=metadata_row[load_type_col],
        merge_keys=metadata_row[merge_keys_col] if layer != "0" else None,
    )

    # ---- 6. Log success ----
    logger.end_run(spark, run_context, status="SUCCESS", **write_result)
    dbutils.notebook.exit("SUCCESS")
except Exception as e:
    # Log the failure, then re-raise so the Databricks Task itself shows
    # as Failed in the Jobs UI -- audit log alone isn't enough, the DAG
    # node needs to go red too.
    logger.end_run(spark, run_context, status="REJECTED", error_message=str(e))
    raise
