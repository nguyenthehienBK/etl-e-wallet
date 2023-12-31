from datetime import timedelta, datetime
from schema.w3_internal_reporting_mart.schema_mart import W3_INTERNAL_REPORTING_TABLE_SCHEMA
from utils.spark_thrift.connections import get_spark_thrift_conn
from airflow.models import DAG
from airflow.operators import IcebergOperator
from airflow.operators import IcebergToMysqlOperator


def get_table_schema(db_mart):
    if db_mart == "w3_internal_reporting":
        ls_tbl = W3_INTERNAL_REPORTING_TABLE_SCHEMA
    return ls_tbl

def sub_load_datamart(parent_dag_name, child_dag_name, args, **kwargs):
    dag_subdag = DAG(
        dag_id="%s.%s" % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
    )
    datamart_name = kwargs.get("datamart_name")
    ls_dim_tbl = [
        'dim_account_state'
        , 'dim_account_tier'
        , 'dim_account_type'
        , 'dim_app_channel'
        , 'dim_area'
        , 'dim_bank'
        , 'dim_bank_account'
        , 'dim_bts_cell'
        , 'dim_country'
        , 'dim_currency'
        , 'dim_date'
        , 'dim_ewallet_service'
        , 'dim_gender'
        , 'dim_identity_document_typ'
        , 'dim_kpi_criteria_code'
        , 'dim_org_type'
        , 'dim_partner'
        , 'dim_reason'
        , 'dim_reason_group'
        , 'dim_strange_behaviour'
        , 'dim_subscriber_type'
        , 'dim_telecom_com'
        , 'dim_telecom_service_type'
        , 'dim_transaction_state'
        , 'dim_transaction_type'
        , 'dim_user'
        , 'dim_user_role'
        , 'dim_user_role_group'
        , 'fact_account'
        , 'fact_transaction'
    ]
    # sql = f"sql/datamart/{datamart_name}/load_{table}_datamart.sql",
    for table in ls_dim_tbl:
        load_table_datamart = IcebergOperator(
            task_id=f"load_{table}_to_datamart",
            execution_timeout=timedelta(hours=2),
            sql=f"SELECT * FROM {datamart_name}.{table}",
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

    ls_mysql_tbl = [
        "mart_tiers"
    ]

    # test load table to mysql
    datamart_name = "w3_core_mdm"
    mysql_schema = [
        {"name": "id", "mode": "NULLABLE", "type": "int"},
        {"name": "description", "mode": "NULLABLE", "type": "varchar(255)"},
        {"name": "role_type_id", "mode": "NULLABLE", "type": "int"},
        {"name": "status", "mode": "NULLABLE", "type": "varchar(255)"},
        {"name": "tier_code", "mode": "NULLABLE", "type": "varchar(255)"},
        {"name": "tier_name", "mode": "NULLABLE", "type": "varchar(255)"},
        {"name": "created_at", "mode": "NULLABLE", "type": "timestamp"},
        {"name": "created_by", "mode": "NULLABLE", "type": "int"},
        {"name": "updated_at", "mode": "NULLABLE", "type": "timestamp"},
        {"name": "updated_by", "mode": "NULLABLE", "type": "int"},
        {"name": "deleted_at", "mode": "NULLABLE", "type": "timestamp"},
        {"name": "deleted_by", "mode": "NULLABLE", "type": "int"},
    ]

    for table in ls_mysql_tbl:
        load_table_datamart = IcebergToMysqlOperator(
            task_id=f"load_table_{table}_to_mart",
            hive_server2_conn_id="hiveserver2_conn_id",
            sql=f"sql/datamart/{datamart_name}/load_{table}_datamart.sql",
            mysql_conn_id="mysql_conn_id_test",
            mysql_database="w3_core_mdm",
            mysql_table_name=table,
            mysql_schema=mysql_schema,
            dag=dag_subdag
        )
        load_table_datamart

    return dag_subdag

def sub_load_mariadb(parent_dag_name, child_dag_name, args, **kwargs):
    dag_subdag = DAG(
        dag_id="%s.%s" % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
        # concurrency=kwargs.get("concurrency"),
    )
    datamart_name = kwargs.get("datamart_name")
    ls_tbl = get_table_schema(db_mart=datamart_name)

    for table in ls_tbl:
        tbl = ls_tbl.get(table)
        # is_fact = True
        table_name = tbl.TABLE_NAME
        schema = tbl.SCHEMA
        load_table_datamart = IcebergToMysqlOperator(
            task_id=f"load_table_{table_name}_to_mart",
            hive_server2_conn_id="hiveserver2_conn_id",
            sql=f"sql/datamart/{datamart_name}/load_{table_name}_datamart.sql",
            mysql_conn_id="mysql_w3_internal_reporting_conn_id",
            mysql_database=datamart_name,
            mysql_table_name=table_name,
            mysql_schema=schema,
            dag=dag_subdag
        )
        load_table_datamart

    return dag_subdag