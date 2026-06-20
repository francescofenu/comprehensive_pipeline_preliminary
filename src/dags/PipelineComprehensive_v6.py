
from datetime import datetime

import sys

sys.path.append("/home/gamma/airflow/modules")

from cosidag import COSIDAG
from cosidag import cfg
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ExternalPythonOperator
from numpy import ndarray

def build_custom(dag):
    # ==============================================
    # 1. External interpreters + library dirs (same conventions as other DAGs)
    # ==============================================
    EXTERNAL_PYTHON_COSIPY = cfg("EXTERNAL_PYTHON_COSIPY", "/home/gamma/envs/cosipy_laura/bin/python")
    LIB_DIR_COMPREHENSIVE_TRANSIENT_PIPELINE = cfg(
        "COMPREHENSIVE_LIB_DIR",
        "/home/gamma/airflow/pipeline/comprehensive-transient-analysis-pipeline.cfmodule/comprehensive_transient_pipeline/",
    )
    
    SOURCE_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='grb_file') }}"
    BACKGROUND_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='background_file') }}"
    ORIENTATION_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='orientation_file') }}"
    RESPONSE_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='response_file') }}"
    #cosipy_yaml_input_file = "/home/gamma/workspace/data/transient/pipeline_zero4_galactic_large_2.yaml"
    cosipy_yaml_input_file = "/home/gamma/workspace//data/pipeline_Comprehensive_GeD.yaml"
    config_pipeline_dir = "/home/gamma/workspace/data/transient/config_pipeline2.txt"

    def binning_data(config_path: str, lib_dir: str):
        import os
        import sys
        import yaml
        sys.path.append(lib_dir)

        from pipeline_functions import execute_bindata_grb
        execute_bindata_grb(config_path,lib_dir,config_txt)

    ged_data_binning = ExternalPythonOperator(
        task_id="Data_Binning",
        python=EXTERNAL_PYTHON_COSIPY,
        python_callable=binning_data,
        op_kwargs={
            #"config_path": "{{ ti.xcom_pull(task_ids='PreProcessing_GeD', key='return_value') }}",
            "config_path": cosipy_yaml_input_file,
            "lib_dir": LIB_DIR_COMPREHENSIVE_TRANSIENT_PIPELINE,
        },
        dag=dag,
    )

    ged_data_binning
    
with COSIDAG(
    dag_id="Comprehensive_Ged_v6",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    monitoring_folders=["/home/gamma/workspace/data/transient2/"],
    file_patterns={
        "grb_file": "data_grbdc3_full.fits"
    },
    select_policy="latest_mtime",
    only_basename="products",
    prefer_deepest=True,
    idle_seconds=5,
    level=3,
    build_custom=build_custom,
    tags=["example"],
):
    pass
