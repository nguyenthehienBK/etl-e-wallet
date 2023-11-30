-- Tạo database
CREATE DATABASE IF NOT EXISTS {{params.iceberg_catalog}}.{{params.bucket_warehouse}};

-- Tạo bảng warehouse
CREATE TABLE IF NOT EXISTS {{params.iceberg_catalog}}.{{params.bucket_warehouse}}.{{params.table_name_warehouse}} (
{{params.create_table_sql}}
)
USING iceberg
LOCATION '{{params.hdfs_path}}'     
TBLPROPERTIES ('write.format.default'='parquet', 'write.parquet.compression-codec'='snappy', 'format-version'='1', 'write.metadata.previous-versions-max'='2', 'external.table.purge'='false', 'write.target-file-size-bytes'='134217728');

-- Insert dữ liệu
MERGE INTO {{params.iceberg_catalog}}.{{params.bucket_warehouse}}.{{params.table_name_warehouse}} t
USING (
    {{params.select_sql}}
    FROM {{params.bucket_staging}}.{{params.table_name_warehouse}}
) s
ON {{params.match_conditions}}
WHEN MATCHED THEN UPDATE SET
    {{params.merge_clause}}
WHEN NOT MATCHED THEN INSERT *;