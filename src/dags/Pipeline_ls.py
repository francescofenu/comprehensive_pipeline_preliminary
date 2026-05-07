from airflow.operators.python import ExternalPythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor

from airflow import DAG

EXTERNAL_PYTHON = "/home/gamma/.conda/envs/cosipy/bin/python"
cosipy_yaml_input_file = "/home/gamma/workspace/data/apps/pipeline_zero4_galactic_large.yaml"
pipeline_configs= "/home/gamma/workspace/data/apps/config_pipeline2.txt"

def execute_bindata_bck_model(cosipy_yaml_input,pipeline_input_file):
    import cosipy

# Default arguments for the DAG
default_args = {
    'owner': 'gamma',
}

with DAG(
    dag_id="Pipeline_ls",
    default_args=default_args,
    description="Run version ls - python version",
    schedule_interval=None,   # run on-demand
    catchup=False,
    tags=["cosifest", "handson", "tutorials"],
) as dag:

    executebinning_bck = ExternalPythonOperator(
        task_id="execute_bindata_bck_model",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=execute_bindata_bck_model,
        op_args=[cosipy_yaml_input_file,pipeline_configs],

    )

    executebinning_bck
    
