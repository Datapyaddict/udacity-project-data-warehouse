import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function 
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This main function loads the data into :
    - the staging tables from the S3 buckets
    - the dimension tables from the staging tables. 
    It uses the connection parameters from 
    the config file.
    """
    print("Get config params") 
    config_file_path = 'dwh.cfg'
    config = configparser.ConfigParser()
    config.read_file(open(config_file_path))
    
    ENDPOINT               = config.get("CLUSTER", "DWH_ENDPOINT")    
    DB_NAME                = config.get("CLUSTER","DWH_DB")
    DB_USER                = config.get("CLUSTER","DWH_DB_USER")
    DB_PASSWORD            = config.get("CLUSTER","DWH_DB_PASSWORD")
    DB_PORT                = config.get("CLUSTER","DWH_PORT")
    
    print("DB connection") 
    conn = psycopg2.connect("host={0} dbname={1} user={2} password={3} port={4}" \
                            .format(ENDPOINT,DB_NAME,DB_USER,DB_PASSWORD,DB_PORT))
    cur = conn.cursor()
    
    print("load staging tables") 
    load_staging_tables(cur, conn)
    
    print("load dimensionm tables")     
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()