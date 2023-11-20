from airflow.models.dag import DAG
from airflow.utils import dates
from airflow.operators.empty import EmptyOperator
from utils.variables.variables_utils import get_variables


DAG_NAME = "01_generic_pipeline"
SCHEDULE_INTERVAL = "00 17 * * *"

with DAG(
        dag_id=DAG_NAME,
        start_date=dates.days_ago(1),
        catchup=False,
        schedule_interval=SCHEDULE_INTERVAL,
        tags=["generic"]
) as dag:
    variables = get_variables(name=DAG_NAME)
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    dummy_task_1 = EmptyOperator(
        task_id="dummy_task_1"
    )

    dummy_task_2 = EmptyOperator(
        task_id="dummy_task_2"
    )

    start >> dummy_task_1 >> dummy_task_2 >> end
