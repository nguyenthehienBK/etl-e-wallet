from airflow.models.dag import DAG
from airflow.utils import dates
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta

DAG_NAME = "01_generic_pipeline"
SCHEDULE_INTERVAL = "00 17 * * *"

args = {
    'owner': 'airflow',
    'start_date': dates.days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

main_dag = DAG(
    dag_id=DAG_NAME,
    default_args=args,
    catchup=False,
    concurrency=4,
    max_active_runs=2,
    schedule_interval=SCHEDULE_INTERVAL
)

start = DummyOperator(task_id="start", dag=main_dag)
end = DummyOperator(task_id="end", dag=main_dag)

dummy_task_1 = DummyOperator(
    task_id="dummy_task_1",
    dag=main_dag
)

dummy_task_2 = DummyOperator(
    task_id="dummy_task_2",
    dag=main_dag
)

start >> dummy_task_1 >> dummy_task_2 >> end
