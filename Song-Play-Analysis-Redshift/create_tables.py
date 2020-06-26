import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """

            The function drops the following tables:
             1. staging_events_table
             2. staging_songs_table
             3. songplay_table
             4. user_table
             5. song_table
             6. artist_table
             7. time_table

            Parameters:
            arg1 (Cursor): Cursor to iterate through create_table_queries list
            arg2 (Connection): Connection Object to connect to Redshift Cluster

            Returns:
            None: Returns nothing, just a function to drop tables and commiting the same

    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Summary line.

        The function creates the following tables:
         1. staging_events_table
         2. staging_songs_table
         3. songplay_table
         4. user_table
         5. song_table
         6. artist_table
         7. time_table

        Parameters:
        arg1 (Cursor): Cursor to iterate through create_table_queries list
        arg2 (Connection): Connection Object to connect to Redshift Cluster

        Returns:
        None: Returns nothing, just a function to create tables and commiting the same

        """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This python file contains methods to drop all the table in Redshift for Song Analysis and create fresh ones
    The file has two main functions:
    1. drop_tables -- To drop all the tables
    2. create_tables -- To create all the tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print(conn)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()