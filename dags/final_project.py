from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator

from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator

from udacity.common.final_project_sql_statements import SqlQueries

default_args = {
    'owner': 'AnhVu',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='anhvu-project4',
        s3_key='log-data',
        json_paths='log_json_path.json'
    )


    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='anhvu-project4',
        s3_key='song-data',
    )
    start_operator >> [stage_songs_to_redshift, stage_events_to_redshift]

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id = 'redshift',
        table = 'songplays',
        sql_query = SqlQueries.songplay_table_insert
    )

    [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id = 'redshift',
        table = 'users',
        sql_query = SqlQueries.user_table_insert,
        truncate_insert=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id = 'redshift',
        table = 'songs',
        sql_query = SqlQueries.song_table_insert,
        truncate_insert=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id = 'redshift',
        table = 'artists',
        sql_query = SqlQueries.artist_table_insert,
        truncate_insert=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id = 'redshift',
        table = 'time',
        sql_query = SqlQueries.time_table_insert,
        truncate_insert=True
    )

    load_songplays_table >> [load_time_dimension_table,load_artist_dimension_table,load_song_dimension_table,load_user_dimension_table]
    
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays','users','artists','time','songs']
    )
    
    [load_time_dimension_table,load_artist_dimension_table,load_song_dimension_table,load_user_dimension_table] >> run_quality_checks

    end_operator = DummyOperator(task_id='End_execution')

    run_quality_checks >> end_operator

final_project_dag = final_project()