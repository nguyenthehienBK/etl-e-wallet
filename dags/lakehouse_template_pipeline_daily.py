from airflow.models import DAG
from utils.variables.variables_utils import get_variables
from airflow.utils import dates
from utils.date_time.date_time_utils import get_business_date
from datetime import timedelta
from utils.dag.dag_utils import CONCURRENCY, MAX_ACTIVE_RUNS
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors import get_default_executor
from sub_dag.subdag_template import sub_load_to_raw

DAG_NAME = "lakehouse_template"
SCHEDULE_INTERVAL = '00 19 * * *'
variables = get_variables(name=DAG_NAME)
BUSINESS_DATE = get_business_date(days=-1, business_date=variables.get("business_date"))
LIST_TABLE_MIGRATION = variables.get('list_table_migration')
variables['business_date'] = BUSINESS_DATE
DELETE_OLD_FILE_RAW_TASK_NAME = 'delete_old_file_raw'
LOAD_TO_RAW_TASK_NAME = 'load_to_raw'
LOAD_TO_STAGING_TASK_NAME = 'load_to_staging'
LOAD_TO_WAREHOUSE_TASK_NAME = 'load_to_warehouse'
START_TASK_NAME = 'start'
END_TASK_NAME = 'end'
OWNER_DAG = 'airflow'

args = {
    'owner': OWNER_DAG,
    'start_date': dates.days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

main_dag = DAG(
    dag_id=DAG_NAME,
    default_args=args,
    schedule_interval=SCHEDULE_INTERVAL,
    concurrency=CONCURRENCY,
    max_active_runs=MAX_ACTIVE_RUNS
)

start_pipeline = DummyOperator(
    task_id=START_TASK_NAME,
    dag=main_dag
)

load_to_raw = SubDagOperator(
    subdag=sub_load_to_raw(
        parent_dag_name=DAG_NAME,
        child_dag_name=LOAD_TO_RAW_TASK_NAME,
        args=args,
        **variables
    ),
    task_id=LOAD_TO_RAW_TASK_NAME,
    executor=get_default_executor(),
    dag=main_dag
)

end_pipeline = DummyOperator(
    task_id=END_TASK_NAME,
    dag=main_dag
)

start_pipeline >> load_to_raw >> end_pipeline
