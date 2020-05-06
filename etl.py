import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This Function iterates over the list of load Queries that load the data from file in S3 bucket
    to the stage tables using copy command
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This Funtion iterates over the list of insert queries that insert the data from stage table to final table.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    It reads the credentials from 'dwh.cfg' file, connect the database,loads the s3 files into stage tables,loads final table
    from stage tables and close the connection.
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