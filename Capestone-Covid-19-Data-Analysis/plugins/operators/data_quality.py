#!/usr/bin/python
# -*- coding: utf-8 -*-

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults




class DataQualityOperator(BaseOperator):
    """
    This class runs the data quality checks against Redshift tables
    """
    ui_color = '#89DA59'


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):
        """
        The __init__ function receives all the arguments from the caller and applies them to current execution cycle
        using the this operations
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id



    def execute(self, context):
        """
        The execute function performs the following steps:
        - Executes the SQL Query for Data Quality check and gets the output of sql query
        - Compares output of SQL query with Expected output
        - If expected output is same as actual output, we see success message in the logs and if not we see an error
        """
        redshift_hook = PostgresHook("redshift")

        for check in self.dq_checks:
            sql = check["check_sql"]
            exp = check["expected_result"]
            records = redshift_hook.get_records(sql)
            exprecords = redshift_hook.get_records(exp)
            num_records = records[0][0]
            exp_records = exprecords[0][0]
            if num_records != exp_records:
                raise ValueError(f"Data quality check failed. Expected: {exp_records} | Got: {num_records}")
            else:
                self.log.info(f"Data quality on SQL {sql} check passed with {records[0][0]} records")
