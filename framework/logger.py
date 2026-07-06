"""
framework/logger.py

Centralized audit logging, matching sql/02_audit_table_ddl.sql exactly.
Every layer script calls start_run() before doing work, and end_run()
inside a try/except wrapper, so every run leaves exactly one row.
"""

import uuid
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import *

AUDIT_TABLE = "{catalog}.audit.ingestion_audit_log"


def _now():
    return datetime.utcnow()


def start_run(spark, catalog, job_run_id, table_id, layer, source_system,
              workspace_url, environment, executed_by,
              reporting_unit_id=None, pipeline_name="gdo_master_workflow",
              notebook_name="orchestrator.py"):
    """Inserts a RUNNING row. Returns a run_context needed by end_run()."""
    audit_id = str(uuid.uuid4())
    start_time = _now()

    row = {
        "audit_id": audit_id,
        "job_run_id": job_run_id,
        "pipeline_name": pipeline_name,
        "notebook_name": notebook_name,
        "table_name": table_id,
        "layer": str(layer),
        "source_system": source_system,
        "reporting_unit_id": reporting_unit_id,
        "environment": environment,
        "workspace_url": workspace_url,
        "executed_by": executed_by,
        "start_time": start_time,
        "end_time": None,
        "duration_seconds": None,
        "status": "RUNNING",
        "records_read": 0,
        "records_inserted": 0,
        "records_updated": 0,
        "records_rejected": 0,
        "error_message": None,
    }

    audit_schema = StructType([
        StructField("audit_id", StringType(), False),
        StructField("job_run_id", StringType(), True),
        StructField("pipeline_name", StringType(), True),
        StructField("notebook_name", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("layer", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("reporting_unit_id", StringType(), True),
        StructField("environment", StringType(), True),
        StructField("workspace_url", StringType(), True),
        StructField("executed_by", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("records_read", LongType(), True),
        StructField("records_inserted", LongType(), True),
        StructField("records_updated", LongType(), True),
        StructField("records_rejected", LongType(), True),
        StructField("error_message", StringType(), True)
    ])
    
    table_name = AUDIT_TABLE.format(catalog=catalog)
    spark.createDataFrame([row], audit_schema)\
    .write\
    .format("delta")\
    .mode("append")\
    .saveAsTable(table_name)

    table_name = AUDIT_TABLE.format(catalog=catalog)
    # spark.createDataFrame([row]).write.format("delta").mode("append").saveAsTable(table_name)

    return {"audit_id": audit_id, "catalog": catalog, "start_time": start_time}


def end_run(spark, run_context, status, records_read=0, records_inserted=0,
            records_updated=0, records_rejected=0, error_message=None):
    """Updates the run's audit row with final status, counts, duration."""
    end_time = _now()
    duration = (end_time - run_context["start_time"]).total_seconds()
    table_name = AUDIT_TABLE.format(catalog=run_context["catalog"])
    safe_error = (error_message or "").replace("'", "''")

    spark.sql(f"""
        UPDATE {table_name}
        SET end_time = '{end_time.isoformat()}',
            duration_seconds = {duration},
            status = '{status}',
            records_read = {records_read},
            records_inserted = {records_inserted},
            records_updated = {records_updated},
            records_rejected = {records_rejected},
            error_message = {f"'{safe_error}'" if error_message else "NULL"}
        WHERE audit_id = '{run_context["audit_id"]}'
    """)