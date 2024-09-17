from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DropTablesOperator(BaseOperator):


    @apply_defaults
    def __init__(self,
                redshift_conn_id = "",
                tables="",
                 *args, **kwargs):

        super(DropTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tables=tables

    def execute(self, context):
        redshift=PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f"Drop table {table}")
            redshift.run(f"DROP TABLE IF EXISTS {table}")