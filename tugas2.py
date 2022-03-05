import mysql.connector as mysql
from mysql.connector import Error
import sqlalchemy
from urllib.parse import quote_plus as urlquote
import matplotlib.pyplot as plt
import pandas as pd


class StudiKasusS2:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def create_db(self, db_name):
        """
        Function to create database
        parameter self is object that contain initial database
        parameter db_name contain name of database that we want to create

        mysql.connect is used to connect the database

        inside the try
        if the connection succes it will create the database by querying with "CREATE DATABASE"
        if the connection failed the error message will be shown
        """
        conn = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password
        )
        try:
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("CREATE DATABASE {}".format(db_name))
        except Error as e:
            print("Error while connecting to MySQL", e)
        # preparing a cursor object
        # creating database

    def create_table(self, db_name, table_name, df):
        """
        Function to create table
        parameter self is object that contain initial database
        parameter db_name contain name of database that we choose
        parameter table_name contain name of table that we want to create
        parameter columns contain column of the table

        mysql.connect is used to connect the database

        inside the try
        if the connection succes it will select the database by querying with "USE" then create the table by querying with "CREATE TABLE"
        if the connection failed the error message will be shown
        """
        conn = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password
        )
        try:
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("USE {}".format(db_name))
                cursor.execute("CREATE TABLE {}".format(table_name))
                # cursor.execute(f"CREATE TABLE {table_name} ({columns})")
        except Error as e:
            print("Error while connecting to MySQL", e)

        engine_stmt = 'mysql+mysqldb://%s:%s@%s:%s/%s' % (self.user, urlquote(self.password),
                                                          self.host, self.port, db_name)
        engine = sqlalchemy.create_engine(engine_stmt)

        df.to_sql(name=table_name, con=engine,
                  if_exists='append', index=False, chunksize=1000)

    # def insert_table(self, db_name, table_name, columns, values):
    #     """
    #     Function to insert table value
    #     parameter self is object that contain initial database
    #     parameter db_name contain name of database that we choose
    #     parameter table_name contain name of table that we want to create
    #     parameter columns contain column of the table
    #     parameter values contain value that want to insert (string)

    #     e.g.
    #     columns = name, address
    #     values = name_value, address_value
    #     mysql.connect is used to connect the database

    #     inside the try
    #     if the connection succes it will select the database by querying with "USE" then insert data in the table by querying with "INSERT INTO"
    #     if the connection failed the error message will be shown
    #     """
    #     conn = mysql.connect(
    #         host=self.host,
    #         port=self.port,
    #         user=self.user,
    #         passwd=self.password
    #     )
    #     try:
    #         conn.autocommit = True
    #         cursor = conn.cursor()
    #         cursor.execute("USE {}".format(db_name))
    #         cursor.execute(
    #             f"INSERT INTO {table_name} ({columns}) VALUES ({values})")
    #         cursor.close()
    #         conn.close()
    #         print("success insert")
    #     except Error as e:
    #         print("Error while connecting to MySQL", e)

    def load_data(self, db_name, table_name):
        """
        Function to insert table value
        parameter self is object that contain initial database
        parameter db_name contain name of database that we choose
        parameter table_name contain name of table that we want to create

        e.g.
        columns = name, address
        values = name_value, address_value
        mysql.connect is used to connect the database

        inside the try
        if the connection succes it will select read the table by querying with "SELECT * FROM" 
        if the connection failed the error message will be shown
        """
        conn = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password
        )
        try:
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM {}.{}".format(db_name, table_name))
                result = cursor.fetchall()
                print("res", result)
                return result
        except Error as e:
            print("Error while connecting to MySQL", e)

    def import_csv(self, path):

        return pd.read_csv(path, index_col=False, delimiter=',')

TesData = StudiKasusS2("127.0.0.1",'3306',"root","")

TesData.create_db("tes_db")

TesData.create_table("tes_db","table_name",TesData.import_csv("tes.csv"))