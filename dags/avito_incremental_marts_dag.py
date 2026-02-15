from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

default_args = {
    'owner': 'avito',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_mart_notebook(**context):
    """
    Запускает ноутбук с параметром ingest_date = вчерашняя дата
    """
    # Получаем вчерашнюю дату в формате YYYY-MM-DD
    yesterday = (context['execution_date'] - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Путь к ноутбуку
    notebook_path = "/workspace/notebooks/work/avito_incremental_marts.ipynb"
    output_path = f"/workspace/notebooks/work/avito_incremental_marts_{yesterday}.ipynb"
    
    # Команда для запуска ноутбука через papermill
    cmd = [
        "papermill",
        notebook_path,
        output_path,
        "-p", "INGEST_DATE", yesterday,
        "--cwd", "/workspace"
    ]
    
    # Устанавливаем переменные окружения для подключения к БД
    env = os.environ.copy()
    env.update({
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "PYTHONPATH": "/workspace"
    })
    
    try:
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            env=env,
            timeout=3600  # Таймаут 1 час
        )
        if result.returncode != 0:
            raise Exception(f"Notebook execution failed:\n{result.stderr}")
        
        print(f"Mart load succeeded for {yesterday}")
        print(f"Output saved to: {output_path}")
        
    except subprocess.TimeoutExpired:
        raise Exception("Notebook execution timed out after 1 hour")
    except Exception as e:
        raise Exception(f"Failed to execute notebook: {str(e)}")

with DAG(
    'avito_incremental_marts',
    default_args=default_args,
    description='Инкрементальная загрузка витрин за вчерашний день',
    schedule_interval='0 2 * * *',  # Ежедневно в 02:00 UTC
    start_date=datetime(2026, 2, 15),  # Начинаем с 15 февраля (загрузка за 14 февраля)
    catchup=False,
    tags=['avito', 'marts', 'incremental'],
) as dag:

    incremental_mart_load = PythonOperator(
        task_id='incremental_mart_load',
        python_callable=run_mart_notebook,
        provide_context=True,
    )

    incremental_mart_load