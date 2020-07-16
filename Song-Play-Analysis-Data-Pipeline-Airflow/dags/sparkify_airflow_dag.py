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
    'start_date': datetime(2020, 7, 15),
    'end_date': datetime(2020, 7, 20),
    'email_on_retry': False
}

# This section defines the DAG and its schedule interval
dag = DAG('song_play_analysis_data_pipeline_airflow',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=3
          )
# This section defines the start operator, this operator doesn't really do anything and is just a dummy start
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# This section defines the operator that reads events data from S3 and loads it to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="public.staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="daayogi-dend",
    s3_key="log_data/2018/11",
    json="s3://daayogi-dend/log_json_path.json",
    createquery=SqlQueries.create_table_staging_events
)
# This section defines the operator that reads songs data from S3 and loads it to Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="public.staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="daayogi-dend",
    s3_key="song_data/A/A/A",
    json="auto",
    createquery=SqlQueries.create_table_staging_songs
)
# This section defines the operator that reads data from staging tables in Redshift and populates Songplays Fact Table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.songplays",
    truncate_table=True,
    query=SqlQueries.songplay_table_insert,
    createquery=SqlQueries.create_table_songplays
)
# This section defines the operator that reads data from staging tables in Redshift and populates Users Dimension Table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.users",
    truncate_table=True,
    query=SqlQueries.user_table_insert,
    createquery=SqlQueries.create_table_users
)
# This section defines the operator that reads data from staging tables in Redshift and populates Songs Dimension Table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.songs",
    truncate_table=True,
    query=SqlQueries.song_table_insert,
    createquery=SqlQueries.create_table_songs
)
# This section defines the operator that reads data from staging tables in Redshift and populates Artist Dimension Table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.artists",
    truncate_table=True,
    query=SqlQueries.artist_table_insert,
    createquery=SqlQueries.create_table_artist
)
# This section defines the operator that reads data from staging tables in Redshift and populates Time Dimension Table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.time",
    truncate_table=True,
    query=SqlQueries.time_table_insert,
    createquery=SqlQueries.create_table_time
)

# This section defines the queries that will be used to perform the Data Quality check
dq_checks = [{'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
             {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}]

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
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> \
load_songplays_table >> [load_user_dimension_table,
                         load_song_dimension_table,
                         load_artist_dimension_table,
                         load_time_dimension_table] \
>> run_quality_checks >> end_operator
