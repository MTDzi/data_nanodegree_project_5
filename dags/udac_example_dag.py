from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries

SCHEMA = 'sparkify'


default_args = {
    'owner': 'MTDzi',
    'start_date': datetime(2020, 4, 15),
    'email': ['mtdziubinski@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'airflow_project',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@daily'
)

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag,
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    schema=SCHEMA,
    table_name='event_staging',
    s3_url='s3://udacity-dend/log_data',
    redshift_conn_id='redshift',
    aws_conn_id='aws_default',
    json_paths='s3://udacity-dend/log_json_path.json',
    autocommit=False,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    schema=SCHEMA,
    table_name='song_staging',
    s3_url='s3://udacity-dend/song_data',
    redshift_conn_id='redshift',
    aws_conn_id='aws_default',
    json_paths='auto',
    autocommit=False,
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
)

end_operator = DummyOperator(
    task_id='End_execution',
    dag=dag,
)

start_operator \
    >> [stage_events_to_redshift, stage_songs_to_redshift] >> end_operator
    # >> load_songplays_table \
    # >> [
    #     load_song_dimension_table,
    #     load_artist_dimension_table,
    #     load_time_dimension_table,
    #     load_user_dimension_table,
    # ] \
    # >> run_quality_checks \
    # >> end_operator
