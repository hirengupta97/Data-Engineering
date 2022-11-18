"""Implement a script to load HHS data in the database"""

import psycopg
import pandas as pd
import numpy as np
import math as m
import sys

conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname="hireng",
    user="hireng", password="aiVee0Ohs"
)

file_name = sys.argv[1]

cur = conn.cursor()
df = pd.read_csv(file_name)

df = df.replace({np.nan: None})

conn.autocommit = True

with conn.transaction():
    for row in range(df.shape[0]):
        hospital_pk = df.iloc[row, 0]
        state = df.iloc[row, 2]
        hospital_name = df.iloc[row, 4]
        address = df.iloc[row, 5]
        city = df.iloc[row, 6]
        zip = df.iloc[row, 7]
        fips_code = df.iloc[row, 9]
        geocoded = (df.iloc[row, 96])
        if (((geocoded is None))):
            longitude = None
            latitude = None
        else:
            longitude = float(geocoded[6:].strip("()").split(" ")[0])
            latitude = float(geocoded[6:].strip("()").split(" ")[1])
        cur.execute(
            "insert into hospital_info (hospital_pk, state, hospital_name,"
            "address, city, zip, fips_code, longitude, latitude)"
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            "ON CONFLICT (hospital_pk) DO NOTHING ",
            (hospital_pk,
                state,
                hospital_name,
                address, city,
                int(zip),
                (fips_code),
                (longitude),
                (latitude)))

num_rows_inserted_info = 0
num_rows_inserted_weekly = 0

missing_values = [-999999, np.nan]
df = pd.read_csv(file_name, na_values=missing_values)


def if_float(n):
    if (m.isnan(n)):
        return None
    else:
        return float(n)


nrow = 0

df2 = pd.DataFrame(
    columns=[
        "hospital_pk",
        "collection_week",
        "all_adult_hospital_beds_7_day_avg",
        "all_pediatric_inpatient_beds_7_day_avg",
        "all_adult_hospital_inpatient_bed_occupied_7_day_coverage",
        "all_pediatric_inpatient_bed_occupied_7_day_avg",
        "total_icu_beds_7_day_avg",
        "icu_beds_used_7_day_avg",
        "inpatient_beds_used_covid_7_day_avg",
        "staffed_icu_adult_patients_confirmed_covid_7_day_avg"])

num_rows_inserted_weekly = 0

with conn.transaction():
    for row in range(df.shape[0]):
        hospital_pk = df.iloc[row, 0]
        collection_week = df.iloc[row, 1]
        all_adult_hospital_beds_7_day_avg = df.iloc[row, 12]
        all_pediatric_inpatient_beds_7_day_avg = df.iloc[row, 111]
        adult_inpatient_bed_occupied_coverage = df.iloc[row, 55]
        pediatric_inpatient_occupied = df.iloc[row, 108]
        total_icu_beds_7_day_avg = df.iloc[row, 22]
        icu_beds_used_7_day_avg = df.iloc[row, 24]
        inpatient_beds_used_covid_7_day_avg = df.iloc[row, 16]
        staffed_icu_adult_confirmed_covid_ = df.iloc[row, 27]
        try:
            with conn.transaction():
                cur.execute(
                    "insert into hospital_weekly(hospital_pk,"
                    "collection_week, all_adult_hospital_beds_7_day_avg,"
                    "all_pediatric_inpatient_beds_7_day_avg,"
                    "all_adult_hospital_inpatient_bed_occupied_7_day_coverage,"
                    "all_pediatric_inpatient_bed_occupied_7_day_avg,"
                    "total_icu_beds_7_day_avg, icu_beds_used_7_day_avg,"
                    "inpatient_beds_used_covid_7_day_avg,"
                    "staffed_icu_adult_patients_confirmed_covid_7_day_avg)"
                    "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (hospital_pk,
                        collection_week,
                        if_float(all_adult_hospital_beds_7_day_avg),
                        if_float(all_pediatric_inpatient_beds_7_day_avg),
                        if_float(adult_inpatient_bed_occupied_coverage),
                        if_float(pediatric_inpatient_occupied),
                        if_float(total_icu_beds_7_day_avg),
                        if_float(icu_beds_used_7_day_avg),
                        if_float(inpatient_beds_used_covid_7_day_avg),
                        if_float(staffed_icu_adult_confirmed_covid_)))
        except Exception as e:
            print("Insert failed!", e)
            df2.loc[nrow] = df.iloc[row,
                                    [0, 1, 12, 111, 55, 108, 22, 24, 16, 27]]
            nrow += 1
        else:
            num_rows_inserted_weekly += 1

print(nrow)
print(num_rows_inserted_weekly)
df2.to_csv("failed_rows_hhs.csv")
