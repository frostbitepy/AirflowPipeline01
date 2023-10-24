from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from pipe_ml_modules import load_data,ml_training_SupportVectorMachine_bigquery, ml_training_KNearestNeighbors_bigquery

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 24),
}

with DAG('pipe_ml_orchestador_dags',
         default_args=default_args,
         schedule_interval='0 23 * * *',
         ) as dag:
         
    inicio = DummyOperator(
            task_id='Inicio_proceso',
        )
        
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_args=["civil-epoch-398922.mlairflowfinal.data_water","gs://automatic-tract-396023-datasetml-f/water_potability.csv"],
    )    

    preprocess = TriggerDagRunOperator(
        task_id='preprocess',
        trigger_dag_id='pipe_ml_dag_preprocess',
        )
        
    with TaskGroup('Model_Training') as Model_Training:
    
        join_start = DummyOperator(
            task_id='start',
        )
        
        # Define la tarea
        ml_training_svm = PythonOperator(
            task_id='ml_training_svm',
            python_callable=ml_training_SupportVectorMachine_bigquery,
            op_args=['civil-epoch-398922', 'mlairflowfinal', 'mlairflow_water_process'],
        )
        
        # Define la tarea
        ml_training_knearest = PythonOperator(
            task_id='ml_training_logistic_regression',
            python_callable=ml_training_KNearestNeighbors_bigquery,
            op_args=['civil-epoch-398922', 'mlairflowfinal', 'mlairflow_water_process'],
        )

        join_task = DummyOperator(
            task_id='join',
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        #join_start >> [training_gradient_boosting, logistic_regression, random_forest] >> join_task
        join_start >> [ml_training_svm, ml_training_knearest] >> join_task         
  

    
    fin = DummyOperator(
            task_id='Fin_proceso',
        )

    inicio >>load_data>>preprocess >> Model_Training >> fin
