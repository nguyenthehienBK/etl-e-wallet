import os
import sys

abs_path = os.path.dirname(os.path.abspath(__file__)) + "/../../../.."
sys.path.append(abs_path)
from airflow.hooks.base_hook import BaseHook
from utils.date_time.date_time_utils import get_business_date
from utils.lakehouse.table_utils import extract_table_info
from utils.database.schemas_utils import template_dlk_valid_table as get_dlk_valid_table
from datetime import timedelta
from airflow.operators import MysqlToHdfsOperator
from utils.lakehouse.lakehouse_layer_utils import (
    RAW,
    WAREHOUSE,
    STAGING,
    ICEBERG,

)
from airflow.models import DAG
from airflow.operators import IcebergOperator

HDFS_CONN_ID = "hdfs_conn_id"
RAW_CONN_ID = "raw_conn_id"
HIVE_SERVER2_CONN_ID = "hive_server2_conn_id"
BUSSINESS_DATE = "business_date"
EXT_DB_SOURCE = "db_source"
EXT_TABLE = "extract_table"
EXCEPT_TABLE = "except_table"


def sub_load_to_raw(parent_dag_name, child_dag_name, args, **kwargs):
    dag = DAG(
        dag_id="%s.%s" % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
    )
    hdfs_conn_id = kwargs.get(HDFS_CONN_ID)
    raw_conn_id = kwargs.get(RAW_CONN_ID)
    db_source = kwargs.get(EXT_DB_SOURCE)
    table = kwargs.get(EXT_TABLE)
    except_table = kwargs.get(EXCEPT_TABLE)
    ls_tbl = get_dlk_valid_table(ls_tbl=table, except_table=except_table)
    for tbl in ls_tbl:
        is_fact = tbl["is_fact"]
        table_name = tbl["name"]
        if is_fact:
            extract_from = kwargs["extract_from"]
            extract_to = kwargs["extract_to"]
            date_info = {"from": extract_from, "to": extract_to}
        else:
            extract_from = get_business_date(days=-1, date_format="%Y-%m-%d")
            extract_to = get_business_date(days=-1, date_format="%Y-%m-%d")
            date_info = {"from": extract_from, "to": extract_to}
        tbl_info = extract_table_info(
            db_source=db_source,
            table_name=table_name,
            is_fact=is_fact,
            etl_from=extract_from,
            etl_to=extract_to,
            hdfs_conn_id=hdfs_conn_id,
            layer="RAW",
            business_day='20231121'
        )
        MysqlToHdfsOperator(
            task_id=f"load_{table_name}_to_raw",
            mysql_conn_id=raw_conn_id,
            hdfs_conn_id=hdfs_conn_id,
            query=tbl_info["sql"]["query"],
            output_path=tbl_info["filename"],
            schema_raw=tbl_info["schema"],
            params=tbl_info["sql"]["params"],
            dag=dag,
        )
    return dag
