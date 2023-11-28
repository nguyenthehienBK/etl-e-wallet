CREATE OR REPLACE TABLE {{params.warehouse}}.dim_profile
USING iceberg
LOCATION '{{params.hdfs_location}}'
AS
SELECT 
*
FROM w3_core_mdm.profile