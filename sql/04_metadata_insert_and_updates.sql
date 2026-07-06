
-- =====================================================================
-- INSERT: one row for DIM_PRODUCT covering all three layers.
-- Replace <YOUR_GIT_FOLDER_PATH> with your actual path, e.g.:
--   /Workspace/Users/you@example.com/gdo-databricks-poc
-- =====================================================================
INSERT INTO uc_dev_snt_fdn.config.ingestion_metadata (
    table_id, source_system, source_schema, source_table, reporting_unit_id,

    layer0_target_catalog, layer0_target_schema, layer0_target_table,
    layer0_script_path, layer0_load_type, layer0_is_active,

    layer1_target_catalog, layer1_target_schema, layer1_target_table,
    layer1_script_path, layer1_load_type, layer1_merge_keys,
    layer1_column_mapping, layer1_transformation_rules, layer1_is_active,

    layer2_target_catalog, layer2_target_schema, layer2_target_table,
    layer2_script_path, layer2_load_type, layer2_merge_keys,
    layer2_column_mapping, layer2_transformation_rules, layer2_is_active,

    partition_column, watermark_column, created_ts, updated_ts
)
VALUES (
    'DIM_PRODUCT', 'FILE', 'source_files',
    '/Volumes/uc_dev_snt_fdn/raw/source_files/dim_product_sample.csv', NULL,

    'uc_dev_snt_fdn', 'raw', 'dim_product_raw',
    '/Workspace/Shared/snowflake_databricks_poc/transformations/layer0/dim_product_l0.py', 'APPEND', true,

    'uc_dev_snt_fdn', 'curated', 'dim_product_curated',
    '/Workspace/Shared/snowflake_databricks_poc/transformations/layer1/dim_product_l1.py', 'MERGE', 'product_id',
    NULL, NULL, true,

    'uc_dev_snt_fdn', 'gold', 'gdo_dim_product',
    '/Workspace/Shared/snowflake_databricks_poc/transformations/layer2/dim_product_l2.py', 'MERGE', 'product_id',
    NULL, NULL, true,

    NULL, NULL, current_timestamp(), current_timestamp()
);

-- =====================================================================
-- Example UPDATEs you'll actually use during development
-- =====================================================================

-- Toggle Layer 2 off temporarily (e.g. gold logic not ready yet)
UPDATE uc_dev_snt_fdn.config.ingestion_metadata
SET layer2_is_active = false, updated_ts = current_timestamp()
WHERE table_id = 'DIM_PRODUCT';

-- Turn it back on
UPDATE uc_dev_snt_fdn.config.ingestion_metadata
SET layer2_is_active = true, updated_ts = current_timestamp()
WHERE table_id = 'DIM_PRODUCT';

-- Fix a script path after moving/renaming a file in your Git folder
UPDATE uc_dev_snt_fdn.config.ingestion_metadata
SET layer1_script_path = '<NEW_PATH>/dim_product_l1.py', updated_ts = current_timestamp()
WHERE table_id = 'DIM_PRODUCT';

-- Point source_table at the real Snowflake table name when you move off the POC
UPDATE uc_dev_snt_fdn.config.ingestion_metadata
SET source_system = 'SNOWFLAKE',
    source_schema = 'SALES',
    source_table = 'DIM_PRODUCT',
    updated_ts = current_timestamp()
WHERE table_id = 'DIM_PRODUCT';
EOF
echo "metadata insert/update queries written"
