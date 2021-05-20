import mysql.connector
from sql_queries import create_tables_queries, drop_tables_queries

def create_database():

    #connect to mysql local database
    conn = mysql.connector.connect( host = 'localhost', user = 'root', password = '32r306b')
    cur = conn.cursor()

    #create sparkify database 
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb")

    # close connection to mysql local database
    conn.close()

    conn = mysql.connector.connect( host = 'localhost', user = 'root', password = '32r306b')
    cur = conn.cursor()
    cur.execute("USE sparkifydb;")

    return cur, conn
    


#dropping tables
def drop_tables(cur, conn):

    for query in drop_tables_queries:
        cur.execute(query)
        conn.commit()


#creating tables
def create_tables(cur, conn):
    for query in create_tables_queries:
        cur.execute(query)
        conn.commit()
    

def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main() 
