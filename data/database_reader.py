import subprocess
import sys
import numpy as np
import pandas as pd
import psycopg2
import psycopg2 as pg
from psycopg2 import OperationalError



connection = pg.connect("host=group3-1-i.comp.nus.edu.sg dbname=healthrecord_encrypted "
                        "port= 5435 user=postgres password=mysecretpassword")
connection.autocommit = True
covid_test = pd.read_sql("select nric,pgp_sym_decrypt(test_result::bytea, 'Y61PAgMBAAE') as test_result from  covid19_test_results", con=connection)
vaccination_results = pd.read_sql("select nric,pgp_sym_decrypt(vaccination_status::bytea, 'Y61PAgMBAAE') as vaccination_status ,pgp_sym_decrypt(vaccine_type ::bytea, 'Y61PAgMBAAE') as vaccine_type from vaccination_results",
                                  con=connection)
particulars = pd.read_sql("select nric,pgp_sym_decrypt(gender::bytea, 'Y61PAgMBAAE') as gender,pgp_sym_decrypt(race::bytea, 'Y61PAgMBAAE') as race,pgp_sym_decrypt(age::bytea, 'Y61PAgMBAAE') as age from user_particulars", con=connection)
address = pd.read_sql("select nric,pgp_sym_decrypt(area::bytea, 'Y61PAgMBAAE') as area from user_address", con=connection)

covid_test['test_result'] = covid_test['test_result'].replace(["0", "1"], ["Negative", "Positive"])
vaccination_results['vaccination_status'] = vaccination_results['vaccination_status'].replace \
    (["0", "1", "2"], ["Not vaccinated", "Partially Vaccinated", "Fully Vaccinated"])
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
headers = ["age","vaccine_type","test_result", "area", "gender", "race","vaccination_status"]
output3['intage'] = output3['age'].astype(int)
output3['age'] = np.where(output3['intage'].between(0,10), "1-10", output3['age'])
output3['age'] = np.where(output3['intage'].between(11,20), "11-20", output3['age'])
output3['age'] = np.where(output3['intage'].between(21,30), "21-30", output3['age'])
output3['age'] = np.where(output3['intage'].between(31,40), "31-40", output3['age'])
output3['age'] = np.where(output3['intage'].between(41,50), "41-50", output3['age'])
output3['age'] = np.where(output3['intage'].between(51,60), "51-60", output3['age'])
output3['age'] = np.where(output3['intage'].between(61,70), "51-60", output3['age'])
output3['age'] = np.where(output3['intage'].between(71,80), "51-60", output3['age'])
output3['age'] = np.where(output3['intage'].between(81,90), "51-60", output3['age'])
output3['age'] = np.where(output3['intage'].between(91,150), "90-", output3['age'])

output3.to_csv('adult.data', index=False, header=False, columns=headers)
print("data read")
