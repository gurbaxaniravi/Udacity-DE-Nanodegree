import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """

                The function loads data from S3 bucket the following tables:
                 1. staging_events_table
                 2. staging_songs_table

                Parameters:
                arg1 (Cursor): Cursor to iterate through create_table_queries list
                arg2 (Connection): Connection Object to connect to Redshift Cluster

                Returns:
                None: Returns nothing, just a function to drop tables and commiting the same

    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """

                The function loads data from staging tables (staging_events table and staging_songs table) to the following tables:
                 1. songplay_table
                 2. user_table
                 3. song_table
                 4. artist_table
                 5. time_table

                Parameters:
                arg1 (Cursor): Cursor to iterate through create_table_queries list
                arg2 (Connection): Connection Object to connect to Redshift Cluster

                Returns:
                None: Returns nothing, just a function to drop tables and commiting the same

    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        This python file contains methods to first load data from S3 to staging tables and then moving data from staging
        tables to final star schema table
        The file has two main functions:
        1. load_staging_tables -- To load tables from S3 to staging tables
        2. insert_tables -- To load table from staging tables to star schema
        """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()