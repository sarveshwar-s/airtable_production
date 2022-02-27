#!/usr/bin/python
# -*- coding: utf-8 -*-
# from airflow import DAG
from airflow.decorators import dag,task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sklearn import linear_model
from get_data_airtable import airtable_data_request, redshift_create_table

import pandas as pd
import os 


@dag(
    dag_id='pipeline_airtabel_airflow', 
    start_date=datetime(2020, 4, 4),
    schedule_interval=timedelta(minutes=2), 
    catchup=False
    )
def pipelines():
    @task(multiple_outputs=True)
    def training_pipeline(**kwargs):
        # ti = kwargs["ti"]
        airtable_app_data = airtable_data_request("App events")
        airtable_web_data = airtable_data_request("Web events")
        assert len(airtable_app_data) != 0
        assert len(airtable_web_data) != 0
        
        return {"app_data": airtable_app_data,"web_data": airtable_web_data}
    
    @task() 
    def update_to_redshift(airtable_app_data, airtable_web_data):
        from get_data_airtable import redshift_create_table
        redshift_create_table("App events",airtable_app_data)
        redshift_create_table("Web events", airtable_web_data)
        print("INSIDE REDSHIFT")
        print(airtable_app_data)
        # ti.xcom_pull(task_id="training_pipeline", key="app_event_xcom_key")
        return "updated to redshift"
    
    @task()
    def update_to_s3():
        from get_data_airtable import upload_files
        data_to_aws = upload_files()
        return "Uploaded to S3"

    airtable_app_data = training_pipeline()
    print("airtable", airtable_app_data["app_data"], airtable_app_data["web_data"])
    database_results = update_to_redshift(airtable_app_data["app_data"], airtable_app_data["web_data"])
    aws_task = update_to_s3()

ml_dag = pipelines()
   
