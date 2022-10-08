from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 postgres_conn_id="",
                 table_name="",
                 sql="",
                 append_data="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.sql = sql
        self.append_data = append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        if self.append_data == True: 
            sql = 'insert into %s %s' %(self.table_name, self.sql)
            redshift.run(sql)
            self.log.info('Data Successfully appended')
        else:
            sql = 'delete from %s' %(self.table_name)
            redshift.run(sql)
            sql = 'insert into %s %s' %(self.table_name, self.sql)
            redshift.run(sql)
            self.log.info('Data Successfully reloaded')           