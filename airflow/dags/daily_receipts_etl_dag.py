from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = 'receipts_daily_load',
    default_args = default_args,
    description = 'Ежедневная инкрементальная генерация чеков',
    start_date = datetime(2026, 4, 1),
    schedule_interval = '0 3 * * *',
    catchup = True,
    tags = ['receipt', 'etl']
) as dag:

    generate_data = BashOperator (
        task_id = 'run_script',
        bash_command = 'python /opt/airflow/dags/daily_receipts_generator.py {{ ds }}'
    )

    load_to_clickhouse = BashOperator (
        task_id = 'load_data_to_clickhouse',
        bash_command = 'python /opt/airflow/dags/load_data_to_clickhouse.py {{ ds }}'
    )

    update_data_for_analytics = BashOperator (
        task_id = 'update_data_for_analytics',
        bash_command = 'python /opt/airflow/dags/load_data_for_analytics.py {{ ds }}'
    )

    generate_data >> load_to_clickhouse >> update_data_for_analytics