#!/usr/bin/python
# -*- coding: utf-8 -*-

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    The purpose of this class is to read data from a S3 location and populate the staging tables in Redshift
    """
    template_fields=("s3_key",)
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER 1 CSV;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 ignore_headers=1,
                 createquery="",
                 *args, **kwargs):
        """
            The __init__ function receives all the arguments from the caller and applies them to current execution cycle
            using the this operations
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.createquery=createquery

    def execute(self, context):
        """
            The execute function performs the following steps:
            - Checks if Staging table in Redshift is already created. If not, it creates one
            - Truncates the staging table so that its a fresh load
            - Reads the data from S3 and loads it into the table
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Creating table in Redshift if the table isn't already created")
        redshift.run("{}".format(self.createquery))
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
        )
        redshift.run(formatted_sql)