import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 bucket to previously created staging tables using the SQL commands in the 'copy_table_queries' list.
    Parameters
    ----------
    cur: cursor 
    conn: connection to database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from staging tables to the previosly created final tables using the SQL commands found in the 'insert_table_queries' list.
    Parameters
    ----------
    cur: cursor 
    conn: connection to database
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Reads the config and connects to the database.
    Loads data from the S3 bucket to staging tables.
    Inserts desired data from the staging tables to the final tables.
    Finally, closes connection to database.
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