from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.papermill_operator import PapermillOperator


default_args = {
    'owner': 'anusha',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 28),
    'email': ['anu.jangalapalli@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': None,
    'catchup': False

}


def jupiter_operator():
    PapermillOperator(
        task_id='housing_data_notebook',
        input_nb='/Users/anusha/Documents/LabNotebookExamples/airflow-pipeline-Anujangalapalli/AirflowHousingData.ipynb',
        output_nb='/Users/anusha/Documents/LabNotebookExamples/airflow-pipeline-Anujangalapalli/out-{{ execution_date }}.ipynb',
        parameters={'msgs': "Ran from Airflow at {{ execution_date }}!"}
    )


dag = DAG('housing_data_workflow',
          default_args=default_args,
          description='Housing Data')

t1 = DummyOperator(task_id='data_workflow_start_here',
                   retries=3,
                   dag=dag)


def call_data():
    print('Housing Data')


t2 = PythonOperator(task_id='housing_data_workflow',
                    python_callable=call_data,
                    dag=dag)

t3 = PythonOperator(

    task_id='Jupiter_notebook',
    python_callable=jupiter_operator,
    dag=dag
)


t1 >> t2 >> t3
