from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 truncate_insert=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_query=sql_query
        self.truncate_insert=truncate_insert

    def execute(self, context):
        redshift=PostgresHook(self.redshift_conn_id)
        self.log.info("LOAD DATA TO DIMENSION {self.table}")
        if self.truncate_insert:
            self.log.info("Truncating dimension table")
            redshift.run("TRUNCATE TABLE {}".format(self.table))
            self.log.info("Inserting data into dimension table")
            redshift.run("INSERT INTO {} {}".format(self.table, self.sql_query))
        else:
            self.log.info("Inserting data into dimension table")
            redshift.run("INSERT INTO {} {}".format(self.table,self.sql_query))
        
