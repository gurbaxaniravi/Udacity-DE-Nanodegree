#!/usr/bin/python
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# This section defines the default arguments that are part of guidelines. 
default_args = {
    'owner': 'gurbaxaniravi',
    'depends_on_past': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 7, 19),
    'end_date': datetime(2020, 7, 21),
    'email_on_retry': False
}

# This section defines the DAG and its schedule interval
dag = DAG('covid_19_capestone_project',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=3
          )
# This section defines the start operator, this operator doesn't really do anything and is just a dummy start
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# This section defines the operator that reads events data from S3 and loads it to Redshift
stage_global_cases_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Global_Cases',
    dag=dag,
    table="public.staging_global_cases",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="daayogi-dend",
    s3_key="JHU_COVID-19.csv",
    createquery=SqlQueries.create_table_staging_global_cases
)
# This section defines the operator that reads songs data from S3 and loads it to Redshift
stage_demographics_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Databank_Demographics',
    dag=dag,
    table="public.staging_databank_demographics",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="daayogi-dend",
    s3_key="DATABANK_DEMOGRAPHICS.csv",
    createquery=SqlQueries.create_table_staging_databank_demographics
)

# This section defines the operator that reads data from staging tables in Redshift and populates Songplays Fact Table
load_globalcases_fact_table = LoadFactOperator(
    task_id='Load_globalcases_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.globalcases_fact",
    truncate_table=True,
    query=SqlQueries.globalcases_fact_table_insert,
    createquery=SqlQueries.create_table_globalcases_fact
)

# This section defines the operator that reads data from staging tables in Redshift and populates Users Dimension Table
load_country_dimension_table = LoadDimensionOperator(
    task_id='Load_countrydemographics_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.countrydemographics_dim",
    truncate_table=True,
    query=SqlQueries.countrydemographics_dim_table_insert,
    createquery=SqlQueries.create_table_countrydemographics_dim
)

# This section defines the operator that reads data from staging tables in Redshift and populates Users Dimension Table
load_date_dimension_table = LoadDimensionOperator(
    task_id='Load_date_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.date_dim",
    truncate_table=True,
    query=SqlQueries.date_dim_table_insert,
    createquery=SqlQueries.create_table_date_dim
)

dq_checks = [{'check_sql': "SELECT COUNT(*) FROM public.date_dim", 'expected_result': "SELECT COUNT(DISTINCT date) FROM public.globalcases_fact" }]

# This section defines the operator that does Data Quality checks
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=dq_checks
)
# This section defines the end operator, this operator doesn't really do anything and is just a dummy end
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# This section defines the flow for the DAG
start_operator >> [stage_global_cases_to_redshift, stage_demographics_to_redshift] >> load_globalcases_fact_table >> \
[load_country_dimension_table,load_date_dimension_table] >> run_quality_checks >> end_operator
