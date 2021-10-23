import pandas as pd
import psycopg2
import psycopg2 as pg
import sys
from psycopg2 import OperationalError, errorcodes, errors
import subprocess


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
covid_test = pd.read_sql("select nric,pgp_sym_decrypt(test_result::bytea, 'mysecretkey') as test_result from  covid19_test_results", con=connection)
vaccination_results = pd.read_sql("select nric,pgp_sym_decrypt(vaccination_status::bytea, 'mysecretkey') as vaccination_status ,pgp_sym_decrypt(vaccine_type ::bytea, 'mysecretkey') as vaccine_type from vaccination_results",
                                  con=connection)
particulars = pd.read_sql("select nric,pgp_sym_decrypt(gender::bytea, 'mysecretkey') as gender,pgp_sym_decrypt(race::bytea, 'mysecretkey') as race,pgp_sym_decrypt(age::bytea, 'mysecretkey') as age from user_particulars", con=connection)
address = pd.read_sql("select nric,pgp_sym_decrypt(area::bytea, 'mysecretkey') as area from user_address", con=connection)

covid_test['test_result'] = covid_test['test_result'].replace(["0", "1"], ["Negative", "Positive"])
vaccination_results['vaccination_status'] = vaccination_results['vaccination_status'].replace \
    ([0, 1, 2], ["Not vaccinated", "Partially Vaccinated", "Fully Vaccinated"])
vaccination_results['vaccine_type'] = vaccination_results['vaccine_type'].replace(["0", "1", "2"] \
                                                                                  , ["pfizer", "moderna", "sinovac"])
particulars['gender'] = particulars['gender'].replace(["0", "1"] \
                                                      , ["female", "male"])
output1 = pd.merge(covid_test, vaccination_results,
                   on='nric',
                   how='inner')
output2 = pd.merge(output1, particulars,
                   on='nric',
                   how='inner')
output3 = pd.merge(output2, address,
                   on='nric',
                   how='inner')
headers = ["age", "vaccination_status", "vaccine_type", "test_result", "area", "gender", "race"]
output3.to_csv('adult.data', index=False, header=False, columns=headers)

subprocess.call("./anonymizer.py", shell=True)

if connection != None:

    try:
        cursor = connection.cursor();
        # Dropping table iris if exists
        cursor.execute("DROP TABLE IF EXISTS public_data;")

        sql = '''CREATE TABLE public_data(
        age varchar NOT NULL, 
        vaccination_status varchar NOT NULL, 
        vaccine_type varchar , 
        test_result varchar NOT NULL,
        area varchar NOT NULL,
        gender varchar NOT NULL,
        race varchar NOT NULL
        )'''

        # Creating a table
        cursor.execute(sql);
        print("public_data table is created successfully................")

        anonymized = pd.read_table("anonymized.data", sep=';',
                                   names=headers)

        # Run the execute_many method
        execute_many(connection, anonymized, 'public_data')

        # Closing the cursor & connection
        cursor.close()
        connection.close()
    except OperationalError as err:
        # pass exception to function
        show_psycopg2_exception(err)
        # set the connection to 'None' in case of error
        conn = None
