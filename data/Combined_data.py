import csv
import random, sys
import pandas as pd
import string
import numpy as np
covid_test = pd.read_csv("covid19_test_results.csv", sep=',',
                         names=["nric", "covid19_test_type", "test_result"])
covid_test['test_result'] = covid_test['test_result'].replace([0,1],["Negative","Positive"])
vaccination_results = pd.read_csv("vaccination_results.csv", sep=',',
                                  names=["second_dose_date", "vaccination_type", "vaccination_centre_location",
                                         "nric", "first_dose_date", "vaccination_status"])
vaccination_results['vaccination_status'] = vaccination_results['vaccination_status'].replace \
    ([0, 1, 2], ["Not vaccinated", "Partially Vaccinated", "Fully Vaccinated"])

particulars = pd.read_csv("user_particulars.csv", sep=',',
                          names=["contact_number", "first_name", "age", "gender",
                                 "date_of_birth", "nric", "last_name", "race"])

address = pd.read_csv("user_address.csv", sep=",",
                      names=["street_name", "nric", "area", "zip_code", "unit_number"])

output1 = pd.merge(covid_test, vaccination_results,
                   on='nric',
                   how='inner')
output2 = pd.merge(output1, particulars,
                   on='nric',
                   how='inner')
output3 = pd.merge(output2, address,
                   on='nric',
                   how='inner')
headers = ["age", "vaccination_status", "test_result", "area", "gender", "race"]
# 0 for not vaccinated, 1 for partially vaccinated, 2 for fully vaccinated
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
