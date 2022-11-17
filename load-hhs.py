"""Implement a script to load HHS data in the database"""

import psycopg
import pandas as pd
import sys
import numpy as np

conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname="ruilinw",
    user="ruilinw", password="Eip8oosei"
)

cur = conn.cursor()

file_name = sys.argv[2]

# read to dataframe
df = pd.read_csv(file_name)
# df.head()
# loc, iloc
df2 = pd.DataFrame()
df2
nrow=0
conn.autocommit = True

num_rows_inserted_info = 0
num_rows_inserted_weekly = 0

# make a new transaction
with conn.transaction():
    for row in range(df.shape[0]):
        hospital_pk = df.iloc[row, 0]
        state = df.iloc[row, 2]
        hospital_name = df.iloc[row, 4]
        address = df.iloc[row, 5]
        city = df.iloc[row, 6]
        zip = df.iloc[row, 7]
        fips_code = df.iloc[row, 9]
        geocoded_hospital_address = df.iloc[row, 96]
#        state, hospital_name, address, city, zip, 
#       fips_code, geocoded_hospital_address = row
        # now insert the data
        cur.execute("insert into hospital_info (hospital_pk, state, hospital_name, address, city, zip, fips_code, geocoded_hospital_address) values (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (hospital_pk, state, hospital_name, address, city, zip, fips_code, geocoded_hospital_address) DO NOTHING",
        (hospital_pk, state, hospital_name, address, city, zip, fips_code, geocoded_hospital_address))

        try:
            # make a new SAVEPOINT -- like a save in a video game
            with conn.transaction():
                # perhaps a bunch of reformatting and data manipulation goes here
                hospital_pk = df.iloc[row, 0]
                collection_week = df.iloc[row, 1]
                all_adult_hospital_beds_7_day_avg = df.iloc[row, 12]
                all_pediatric_inpatient_beds_7_day_avg = df.iloc[row, 111]
                all_adult_hospital_inpatient_bed_occupied_7_day_coverage = df.iloc[row, 55]
                all_pediatric_inpatient_bed_occupied_7_day_avg = df.iloc[row, 108]
                total_icu_beds_7_day_avg = df.iloc[row, 22]
                icu_beds_used_7_day_avg = df.iloc[row, 24]
                inpatient_beds_used_covid_7_day_avg = df.iloc[row, 16]
                staffed_icu_adult_patients_confirmed_covid_7_day_avg = df.iloc[row, 27]
#                hospital_pk, collection_week, all_adult_hospital_beds_7_day_avg, all_pediatric_inpatient_beds_7_day_avg, all_adult_hospital_inpatient_bed_occupied_7_day_coverage, all_pediatric_inpatient_bed_occupied_7_day_avg, total_icu_beds_7_day_avg, icu_beds_used_7_day_avg, inpatient_beds_used_covid_7_day_avg, staffed_icu_adult_patients_confirmed_covid_7_day_avg = row
                # now insert the data
                cur.execute("insert into hospital_weekly(hospital_pk, collection_week, all_adult_hospital_beds_7_day_avg, all_pediatric_inpatient_beds_7_day_avg, all_adult_hospital_inpatient_bed_occupied_7_day_coverage, all_pediatric_inpatient_bed_occupied_7_day_avg, total_icu_beds_7_day_avg, icu_beds_used_7_day_avg, inpatient_beds_used_covid_7_day_avg, staffed_icu_adult_patients_confirmed_covid_7_day_avg) " 
                            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (hospital_pk, collection_week, all_adult_hospital_beds_7_day_avg, all_pediatric_inpatient_beds_7_day_avg, all_adult_hospital_inpatient_bed_occupied_7_day_coverage, all_pediatric_inpatient_bed_occupied_7_day_avg, total_icu_beds_7_day_avg, icu_beds_used_7_day_avg, inpatient_beds_used_covid_7_day_avg, staffed_icu_adult_patients_confirmed_covid_7_day_avg))
        except Exception as e:
            # if an exception/error happens in this block, Postgres goes back to
            # the last savepoint upon exiting the `with` block
            print("insert failed")
            df2.loc[nrow] = df.loc[row]
            nrow += 1
            # add additional logging, error handling here
        else:
            # no exception happened, so we continue without reverting the savepoint
            num_rows_inserted_weekly += 1

# now we commit the entire transaction
# conn.commit()

df2.columns = ["hospital_pk", "collection_week", "all_adult_hospital_beds_7_day_avg", "all_pediatric_inpatient_beds_7_day_avg", "all_adult_hospital_inpatient_bed_occupied_7_day_coverage", "all_pediatric_inpatient_bed_occupied_7_day_avg", "total_icu_beds_7_day_avg", "icu_beds_used_7_day_avg", "inpatient_beds_used_covid_7_day_avg", "staffed_icu_adult_patients_confirmed_covid_7_day_avg"]
df2.to_csv
