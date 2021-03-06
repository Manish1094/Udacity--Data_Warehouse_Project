# Import Libraries  
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Drop Tables function will call the queries to drop the fact & dimension tables 

def drop_tables(cur, conn):
     """
        This function iterates over all the drop table queries and executes them.
        INPUTS:
        * cur the cursor variable of the database
        * conn the connection variable of the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
# Create Table function will call the queries to create the fact & dimension

def create_tables(cur, conn):
    """
        This function iterates over all the create table queries and executes them.
        INPUTS:
        * cur the cursor variable of the database
        * conn the connection variable of the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
       Main Function connects to the redshift cluster which has already been created & started using the host'
     """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()