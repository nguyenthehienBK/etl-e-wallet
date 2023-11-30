import os
import sys

abs_path = os.path.dirname(os.path.abspath(__file__)) + "/../../../.."
sys.path.append(abs_path)

from datetime import timedelta
from airflow.models import DAG
from airflow.operators import OracleToHdfsOperator
from airflow.operators import IcebergOperator
from schema.lakehouse_template.schema_dlk import TEMPLATE_TABLE_SCHEMA
from schema.w3_system_accounting.schema_dlk import W3_SYSTEM_ACCOUNTING_TABLE_SCHEMA
from schema.w3_cp_payment_business.schema_dlk import W3_CP_PAYMENT_BUSINESS_TABLE_SCHEMA
from utils.date_time.date_time_utils import get_business_date
from utils.lakehouse.table_utils import get_hdfs_path, get_sql_param, get_host_port, get_merge_query_dwh
from utils.lakehouse.lakehouse_layer_utils import (
    RAW,
    WAREHOUSE,
    STAGING,
    ICEBERG,
    BRONZE,
    SILVER,
    GOLD,
)

HDFS_CONN_ID = "hdfs_conn_id"
RAW_CONN_ID = "raw_conn_id"
HIVE_SERVER2_CONN_ID = "hive_server2_conn_id"
BUSINESS_DATE = "business_date"
EXT_DB_SOURCE = "db_source"
EXT_TABLE = "extract_table"
EXCEPT_TABLE = "except_table"


def get_table_schema(db_source):
    ls_tbl = TEMPLATE_TABLE_SCHEMA
    if db_source == "w3_system_accounting":
        ls_tbl = W3_SYSTEM_ACCOUNTING_TABLE_SCHEMA
    if db_source == "w3_cp_payment_business":
        ls_tbl = W3_CP_PAYMENT_BUSINESS_TABLE_SCHEMA
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
                                    layer="BRONZE", bucket=db_source, business_day=business_date)
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
        OracleToHdfsOperator(
            task_id=f"load_{table_name}_to_raw",
            oracle_conn_id=raw_conn_id,
            hdfs_conn_id=hdfs_conn_id,
            query=query,
            output_path=output_path,
            schema_raw=schema,
            params=params,
            dag=dag,
        )
    return dag


def sub_load_to_staging(parent_dag_name, child_dag_name, args, **kwargs):
    dag = DAG(
        dag_id="%s.%s" % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
    )
    hdfs_conn_id = kwargs.get(HDFS_CONN_ID)
    db_source = kwargs.get(EXT_DB_SOURCE)
    business_date = kwargs.get("business_date")
    ls_tbl = get_table_schema(db_source=db_source)
    for table in ls_tbl:
        tbl = ls_tbl.get(table)
        table_name = tbl.TABLE_NAME
        output_path = get_hdfs_path(table_name=table_name, hdfs_conn_id=hdfs_conn_id,
                                    layer="BRONZE", bucket=db_source, business_day=business_date)
        sql = "dags/sql/template/load_staging_template.sql"
        host, port = get_host_port(hdfs_conn_id=hdfs_conn_id)
        IcebergOperator(
            task_id=f"load_{table_name}_to_staging",
            execution_timeout=timedelta(hours=2),
            sql=sql,
            hive_server2_conn_id=kwargs.get(HIVE_SERVER2_CONN_ID),
            params={
                "iceberg_catalog": ICEBERG,
                "bucket_lakehouse": f"{db_source}",
                "bucket_staging": f"{db_source}_staging",
                "bucket_warehouse": f"{db_source}_datawarehouse",
                "business_date": kwargs.get(BUSINESS_DATE),
                "table_name_raw": table_name,
                "table_name_warehouse": table_name,
                "raw_layer": BRONZE,
                "hdfs_host": host,
                "hdfs_port": port,
                "hdfs_path": output_path,
            },
            dag=dag,
        )
    return dag


def sub_load_to_warehouse(parent_dag_name, child_dag_name, args, **kwargs):
    dag = DAG(
        dag_id="%s.%s" % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
    )
    hdfs_conn_id = kwargs.get(HDFS_CONN_ID)
    db_source = kwargs.get(EXT_DB_SOURCE)
    business_date = kwargs.get("business_date")
    ls_tbl = get_table_schema(db_source=db_source)
    for table in ls_tbl:
        tbl = ls_tbl.get(table)
        table_name = tbl.TABLE_NAME
        output_path = get_hdfs_path(table_name=table_name, hdfs_conn_id=hdfs_conn_id,
                                    layer=SILVER, bucket=db_source, business_day=business_date)
        sql = "dags/sql/template/load_to_warehouse_template.sql"
        host, port = get_host_port(hdfs_conn_id=hdfs_conn_id)
        sql_param = get_merge_query_dwh(tbl=tbl)
        IcebergOperator(
            task_id=f"load_{table_name}_to_warehouse",
            execution_timeout=timedelta(hours=2),
            sql=sql,
            hive_server2_conn_id=kwargs.get(HIVE_SERVER2_CONN_ID),
            params={
                "iceberg_catalog": ICEBERG,
                "bucket_lakehouse": f"{db_source}",
                "bucket_staging": f"{db_source}_staging",
                "bucket_warehouse": f"{db_source}_datawarehouse",
                "business_date": kwargs.get(BUSINESS_DATE),
                "table_name_raw": table_name,
                "table_name_warehouse": table_name,
                "raw_layer": BRONZE,
                "hdfs_host": host,
                "hdfs_port": port,
                "hdfs_path": output_path,
                "create_table_sql": sql_param["create_table_sql"],
                "select_sql": sql_param["select_sql"],
                "match_conditions": sql_param["match_conditions"],
                "merge_clause": sql_param["merge_clause"]
            },
            dag=dag,
        )
    return dag
