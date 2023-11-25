import os
import sys

abs_path = os.path.dirname(os.path.abspath(__file__)) + "/../../../.."
sys.path.append(abs_path)
from airflow.hooks.base_hook import BaseHook
from utils.date_time.date_time_utils import get_business_date
from utils.lakehouse.table_utils import get_hdfs_path, get_sql_param
from datetime import timedelta
from schema.lakehouse_template.schema_dlk import TEMPLATE_TABLE_SCHEMA
from schema.generic.schema_dlk import GENERIC_TABLE_SCHEMA
from schema.w3_core_mdm.schema_dlk import W3_CORE_MDM_TABLE_SCHEMA
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


def get_table_schema(db_source):
    ls_tbl = TEMPLATE_TABLE_SCHEMA
    if db_source == "w3_core_mdm":
        ls_tbl = W3_CORE_MDM_TABLE_SCHEMA
    return ls_tbl


def sub_load_to_raw(parent_dag_name, child_dag_name, args, **kwargs):
    dag = DAG(
        dag_id="%s.%s" % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
    )
    hdfs_conn_id = kwargs.get(HDFS_CONN_ID)
    raw_conn_id = kwargs.get(RAW_CONN_ID)
    db_source = kwargs.get(EXT_DB_SOURCE)
    business_date = kwargs.get("business_date")
    ls_ext_table = kwargs.get(EXT_TABLE)
    ls_tbl = get_table_schema(db_source=db_source)
    for table in ls_tbl:
        tbl = ls_tbl.get(table)
        is_fact = True
        table_name = tbl.TABLE_NAME
        schema = tbl.SCHEMA_RAW
        if is_fact:
            extract_from = kwargs["extract_from"]
            extract_to = kwargs["extract_to"]
            date_info = {"from": extract_from, "to": extract_to}
        else:
            extract_from = get_business_date(days=-1, date_format="%Y-%m-%d")
            extract_to = get_business_date(days=-1, date_format="%Y-%m-%d")
            date_info = {"from": extract_from, "to": extract_to}

        output_path = get_hdfs_path(table_name=table_name, hdfs_conn_id=hdfs_conn_id,
                                    layer="RAW", bucket=db_source, business_day=business_date)
        # query = get_sql_param(tbl=tbl).get("query")
        query = """
        SELECT
            {{params.columns}}
            FROM {{params.table_name}}
            {{params.join}}
            {{params.where_condition}}
            ORDER BY {{params.order_by}}
        """
        params = get_sql_param(tbl=tbl).get("params")
        MysqlToHdfsOperator(
            task_id=f"load_{table_name}_to_raw",
            mysql_conn_id=raw_conn_id,
            hdfs_conn_id=hdfs_conn_id,
            query=query,
            output_path=output_path,
            schema_raw=schema,
            params=params,
            dag=dag,
        )
    return dag
