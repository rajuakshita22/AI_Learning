import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2

#load .env file
load_dotenv() #python package to load environment variables from a .env file



def getConnection():
    #Read environment variables
    HOST = os.getenv("DB_HOST")
    PORT = os.getenv("DB_PORT")
    DATABASE = os.getenv("DB_NAME")
    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")
    return psycopg2.connect(
            host = HOST,
            port = PORT,
            dbname = DATABASE,
            user = USER,
            password = PASSWORD
        )

def get_tableRows(query):

    try:
        # COnnect to PostgreSQL
        connection = getConnection()

        #Read data into a pandas DataFrame
        df = pd.read_sql(query, connection)
        print(df)

    except Exception as e:
        print("Error:",e)

    finally:
        if 'connection' in locals():
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    query = 'SELECT * FROM "your_table" LIMIT 5;'
    get_tableRows(query)