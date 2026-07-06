"""
framework/write_utils.py

Shared write logic for every layer. Handles APPEND (Layer 0) vs MERGE
(Layer 1/2) and returns precise insert/update counts for the audit log
by reading Delta's own operation metrics -- no manual counting needed.
"""

from delta.tables import DeltaTable


def write_and_publish(spark, df, target_catalog, target_schema, target_table,
                       load_type, merge_keys=None):
    """
    Writes df to <target_catalog>.<target_schema>.<target_table>.
    Returns a dict: {"records_read", "records_inserted", "records_updated"}.
    """
    full_table_name = f"{target_catalog}.{target_schema}.{target_table}"
    records_read = df.count()

    if load_type.upper() == "APPEND":
        df.write.format("delta").mode("append").saveAsTable(full_table_name)
        return {"records_read": records_read, "records_inserted": records_read, "records_updated": 0}

    elif load_type.upper() == "MERGE":
        if not merge_keys:
            raise ValueError(f"MERGE load_type requires merge_keys for {full_table_name}")

        if not spark.catalog.tableExists(full_table_name):
            # First run: table doesn't exist yet, create it directly
            df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
            return {"records_read": records_read, "records_inserted": records_read, "records_updated": 0}

        keys = [k.strip() for k in merge_keys.split(",")]
        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in keys])

        target = DeltaTable.forName(spark, full_table_name)
        (target.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())

        # Pull exact insert/update counts from Delta's own transaction history
        history = spark.sql(f"DESCRIBE HISTORY {full_table_name} LIMIT 1").collect()[0]
        metrics = history["operationMetrics"] or {}
        inserted = int(metrics.get("numTargetRowsInserted", 0))
        updated = int(metrics.get("numTargetRowsUpdated", 0))

        return {"records_read": records_read, "records_inserted": inserted, "records_updated": updated}

    else:
        raise ValueError(f"Unknown load_type '{load_type}' for {full_table_name}")