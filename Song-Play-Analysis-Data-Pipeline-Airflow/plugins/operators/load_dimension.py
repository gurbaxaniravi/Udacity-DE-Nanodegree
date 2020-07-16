from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This class reads data from Redshift Staging tables and populates the Dimension tables in Redshift
    """

    ui_color = '#80BD9E'


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 truncate_table=True,
                 query="",
                 createquery="",
                 *args, **kwargs):
        """
        The __init__ function receives all the arguments from the caller and applies them to current execution cycle
        using the this operations
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id = redshift_conn_id
        self.truncate_table=truncate_table
        self.query=query
        self.createquery=createquery


    def execute(self, context):
        """
        The execute function performs the following steps:
        - Checks if the dimension table in Redshift is already created. If not, it creates one
        - Truncates the dimension table so that its a fresh load
        - Reads the data from Redshift staging table and loads it into the dimension table
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Creating table in Redshift if the table isn't already created")
        redshift.run("{}".format(self.createquery))
        if self.truncate_table:
            self.log.info(f'Truncating Table {self.table}')
            redshift.run("DELETE FROM {}".format(self.table))
        self.log.info(f'Running query {self.query}')
        redshift.run(f"Insert into {self.table} {self.query}")
