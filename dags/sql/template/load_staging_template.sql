-- Tạo database
CREATE DATABASE IF NOT EXISTS {{params.iceberg_catalog}}.{{params.bucket_staging}};

-- Xóa dữ liệu bảng cũ
DROP TABLE IF EXISTS {{params.bucket_staging}}.{{params.table_name_warehouse}}_stg;

-- Tạo bảng tmp 
CREATE TABLE IF NOT EXISTS {{params.bucket_staging}}.{{params.table_name_warehouse}}_stg
USING org.apache.spark.sql.parquet 
LOCATION '{{params.hdfs_path}}';