import pendulum
from datetime import timedelta,datetime
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from final_project_operators.create_tables_operator import CreateTablesOperator
from final_project_operators.drop_tables_operator import DropTablesOperator

default_args = {
    'owner': 'AnhVu',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Create tables in Redshift with Airflow',
    schedule_interval='@hourly',
)
def create_tables():
    start_operator = DummyOperator(task_id='Begin_execution')

    create_redshift_tables = CreateTablesOperator(
        task_id='Create_tables',
        redshift_conn_id='redshift',
    )

    drop_redshift_tables = DropTablesOperator(
        task_id='drop_tables',
        redshift_conn_id='redshift',
        tables = ['staging_events', 'staging_songs', 'users', 'songs', 'artists', 'songplays', 'time']
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> drop_redshift_tables >> create_redshift_tables >> end_operator


create_tables_dag = create_tables()