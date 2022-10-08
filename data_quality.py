import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 quality_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.quality_checks = quality_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for quality_checks in self.quality_checks:
            sql = quality_checks.get('sql')
            count_condition = quality_checks.get('count_condition')
            records = redshift_hook.get_records(sql)
            if records[0][0] > count_condition:
                self.log.info(f"Data quality check passed. Sql is {sql}. Count is {records[0][0]}")
            else:
                raise ValueError(f"Data quality check failed. Sql is {sql}. Count is {records[0][0]}")
        self.log.info('Data quality check finished')