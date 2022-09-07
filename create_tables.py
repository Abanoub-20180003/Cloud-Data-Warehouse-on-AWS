import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print(query," Done")


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print(query," Done")

def connect_to_db(config):
    # get db connection info
    HOST= config.get("DWH","HOST")
    DB_NAME= config.get("DWH","DB_NAME")
    DB_USER= config.get("DWH","DB_USER")
    DB_PASSWORD= config.get("DWH","DB_PASSWORD")
    DB_PORT= config.get("DWH","DB_PORT")
    
    # connect to database 
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST,DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))
    cur = conn.cursor()
    return conn, cur
    
    
def main():
    config = configparser.ConfigParser()
    config.read('dwh2.cfg')
    
    #connect to db
    conn, cur = connect_to_db(config)
    print("Connect To DB Done")
    
    #drop and create tables 
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()