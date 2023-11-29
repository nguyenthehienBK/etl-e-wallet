from airflow.models import DAG
from airflow.operators import IcebergToMysqlOperator
from utils.dag.dag_utils import CONCURRENCY, MAX_ACTIVE_RUNS
from datetime import timedelta
from airflow.utils import dates

OWNER_DAG = 'airflow'

args = {
    'owner': OWNER_DAG,
    'start_date': dates.days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

DAG_NAME = "test_operator"
SCHEDULE_INTERVAL = '00 19 * * *'

main_dag = DAG(
    dag_id=DAG_NAME,
    default_args=args,
    schedule_interval=SCHEDULE_INTERVAL,
    concurrency=CONCURRENCY,
    max_active_runs=MAX_ACTIVE_RUNS
)

schema = [
    {"name": "id", "mode": "NULLABLE", "type": "int"},
    {"name": "role_type_id", "mode": "NULLABLE", "type": "int"},
    {"name": "order_id", "mode": "NULLABLE", "type": "string"},
]
test_operator_1 = IcebergToMysqlOperator(
    task_id="test_operator",
    hive_server2_conn_id="hiveserver2_conn_id",
    sql="select id, role_type_id, status from w3_core_mdm_staging.tiers",
    mysql_conn_id="mysql_conn_id_test",
    mysql_database="test_operator",
    mysql_table="test_table",
    mysql_schema=schema,
    dag=main_dag

)
