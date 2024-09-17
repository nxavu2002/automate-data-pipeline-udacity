from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from helpers import create_tables_query
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):


    @apply_defaults
    def __init__(self,
                redshift_conn_id = "",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id

    def execute(self, context):
        redshift=PostgresHook(self.redshift_conn_id)
        self.log.info(f"Create table")
        redshift.run(create_tables_query.create_tables_sql)