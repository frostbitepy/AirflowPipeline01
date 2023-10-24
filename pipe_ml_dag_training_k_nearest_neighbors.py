from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pipe_ml_modules import ml_training_KNearestNeighbors_bigquery
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# Definir los argumentos predeterminados
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define el DAG
with DAG('pipe_ml_dag_training_k_nearest_neighbors',
         default_args=default_args,
         description='K Nearest Neighbors',
         schedule_interval=None,
         ) as dag:

    # Define la tarea
    ml_training_knearest = PythonOperator(
        task_id='ml_training_knearest',
        python_callable=ml_training_KNearestNeighbors_bigquery,
        op_args=['civil-epoch-398922', 'mlairflowfinal', 'mlairflow_water_process'],
    )

    # Establecer las dependencias
    ml_training_knearest
