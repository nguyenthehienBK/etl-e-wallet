import os
import sys
abs_path = os.path.dirname(os.path.abspath(__file__)) + '/../..'
sys.path.append(abs_path)

RAW = "raw"
STAGING = "staging"
WAREHOUSE = "warehouse"
MART = "mart"
BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"

ICEBERG = 'iceberg'
PARQUET_FORMAT = 'parquet'


