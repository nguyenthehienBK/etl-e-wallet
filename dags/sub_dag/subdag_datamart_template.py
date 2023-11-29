from datetime import timedelta, datetime
from utils.spark_thrift.connections import get_spark_thrift_conn
from airflow.models import DAG
from airflow.operators import IcebergOperator
from airflow.operators import IcebergToMysqlOperator
from schema.w3_core_mdm.schema_mart import *


def sub_load_datamart(parent_dag_name, child_dag_name, args, **kwargs):
    dag_subdag = DAG(
        dag_id="%s.%s" % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
    )
    datamart_name = kwargs.get("datamart_name")
    ls_dim_tbl = [
        "dim_account",
        "dim_profile"
    ]

    for table in ls_dim_tbl:
        load_table_datamart = IcebergOperator(
            task_id=f"load_{table}_to_datamart",
            execution_timeout=timedelta(hours=2),
            sql=f"sql/datamart/{datamart_name}/load_{table}_datamart.sql",
            hive_server2_conn_id="hiveserver2_default_1",
            dag=dag_subdag,
            iceberg_db=f"iceberg.{datamart_name}",
            params={
                "hdfs_location": f"/{datamart_name}/{table}",
                "warehouse": f"/{datamart_name}",
            }
        )
        load_table_datamart
    return dag_subdag


def sub_load_mysql(parent_dag_name, child_dag_name, args, **kwargs):
    dag_subdag = DAG(
        dag_id="%s.%s" % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
        # concurrency=kwargs.get("concurrency"),
    )
    # datamart_name = kwargs.get("datamart_name")

    # ls_mysql_tbl = [
    #     "dim_account",
    #     "dim_profile"
    # ]

    datamart_name = "w3_core_mdm"
    ls_mysql_tbl = W3_CORE_MDM_TABLE_SCHEMA()

    for table in ls_mysql_tbl:
        tbl = ls_mysql_tbl.get(table)
        table_name = tbl.TABLE_NAME
        mysql_schema = tbl.SCHEMA_MYSQL
        load_table_datamart = IcebergToMysqlOperator(
            task_id=f"load_table_{table_name}_to_mart",
            hive_server2_conn_id="hiveserver2_conn_id",
            sql=f"sql/datamart/{datamart_name}/load_{table_name}_datamart",
            mysql_conn_id="mysql_conn_id_test",
            mysql_database="w3_core_mdm",
            mysql_table_name="tiers_mart",
            mysql_schema=mysql_schema,
            dag=dag_subdag)
        load_table_datamart

    return dag_subdag
