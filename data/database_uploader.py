import subprocess
import sys
import numpy as np
import pandas as pd
import psycopg2
import psycopg2 as pg
from psycopg2 import OperationalError


def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()
    # get the line number when exception occured
    line_n = traceback.tb_lineno
    # print the connect() error
    print("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print("psycopg2 traceback:", traceback, "-- type:", err_type)
    # psycopg2 extensions.Diagnostics object attribute
    print("\nextensions.Diagnostics:", err.diag)
    # print the pgcode and pgerror exceptions
    print("pgerror:", err.pgerror)
    print("pgcode:", err.pgcode, "\n")


def execute_many(conn, datafrm, table):
    # Creating a list of tupples from the dataframe values
    tpls = [tuple(x) for x in datafrm.to_numpy()]

    # dataframe columns with Comma-separated
    cols = ','.join(list(datafrm.columns))

    # SQL query to execute
    sql = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(sql, tpls)
        conn.commit()
        print("Data inserted using execute_many() successfully...")
    except (Exception, psycopg2.DatabaseError) as err:
        # pass exception to function
        show_psycopg2_exception(err)
        cursor.close()


connection = pg.connect("host=group3-1-i.comp.nus.edu.sg dbname=healthrecord_encrypted "
                        "port= 5435 user=postgres password=mysecretpassword")
connection.autocommit = True


if connection != None:

    try:
        cursor = connection.cursor();
        # Dropping table  if exists
        cursor.execute("DROP TABLE IF EXISTS public_data;")

        sql = '''CREATE TABLE public_data(
        age varchar NOT NULL,  
        test_result varchar NOT NULL,
        gender varchar NOT NULL,
        vaccination_status varchar NOT NULL,
        area varchar NOT NULL,
        race varchar NOT NULL,
        vaccine_type varchar NOT NULL
        )'''

        # Creating a table
        cursor.execute(sql);
        print("public_data table is created successfully................")
        aheaders = ["area", "race", "vaccine_type"]
        headers = ["age", "vaccine_type", "test_result", "area", "gender", "race", "vaccination_status"]
        anonymized = pd.read_table("anonymized.data", sep=';',
                                   names=aheaders)
        adult_table = pd.read_table("adult.data", sep=',',
                                    names=headers)
        sub_df = adult_table.drop(columns=["area", "race", "vaccine_type"])

        anonymized['C'] = np.arange(len(anonymized))
        sub_df['C'] = np.arange(len(sub_df))
        df = pd.merge(sub_df, anonymized, on='C',how="inner")
        df = df.drop('C', axis=1)

        # Run the execute_many method
        execute_many(connection, df, 'public_data')

        # Closing the cursor & connection
        cursor.close()
        connection.close()
    except OperationalError as err:
        # pass exception to function
        show_psycopg2_exception(err)
        # set the connection to 'None' in case of error
        conn = None
