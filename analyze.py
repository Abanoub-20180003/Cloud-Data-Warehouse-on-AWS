import psycopg2
import configparser
from sql_queries import analyze_queries


def analyze_query(cur, conn):
    """
    Run the queries in the 'analyze_queries' list on staging and dimensional tables.
    :param cur: cursor object to database connection
    :param conn: connection object to database
    """
    
    for query in analyze_queries:
        print('Running ' + query)         
        try:
            cur.execute(query)
            results = cur.fetchone()

            for row in results:
                print("   ", row)
                conn.commit()
                
        except psycopg2.Error as e:
            print(e)
            conn.close()

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
    """
    Run COUNT(*) query on the staging and dimensional tables to validate that the data has been loaded into Redshift
    """
    
    config = configparser.ConfigParser()
    config.read('dwh2.cfg')

    conn, cur = connect_to_db(config)
    
    analyze_query(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()