"""Implement a script to load quality data in the database"""

import psycopg
import pandas as pd
import numpy as np
import sys

conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname="hireng",
    user="hireng", password="aiVee0Ohs"
)

file_name = sys.argv[2]
date = sys.argv[1]

cur = conn.cursor()

missing_values = [-999999, np.nan]
df = pd.read_csv(file_name, na_values=missing_values)
df.loc[df["Hospital overall rating"]== "Not Available"]=np.nan

conn.autocommit = True

num_rows_inserted_quality = 0


def if_float_for_str(n):
    if (type(n) == str):
        return float(n)
    else:
        return None


nrow = 0

df2 = pd.DataFrame(columns=[
    "hospital_pk, type_of_hospital,"
    "ownership, emergency, overall_quality_rating"])

num_rows_inserted_quality = 0

with conn.transaction():
    for row in range(df.shape[0]):
        facility_id = df.iloc[row, 0]
        time = date
        type_of_hospital = df.iloc[row, 8]
        ownership = df.iloc[row, 9]
        emergency = df.iloc[row, 10]
        overall_quality_rating = df.iloc[row, 12]
        try:
            with conn.transaction():
                cur.execute(
                    "insert into hospital_quality"
                    "(facility_id, update_time, type_of_hospital,"
                    "ownership, emergency, overall_quality_rating)"
                    "values ( %s, %s, %s, %s, %s, %s)",
                    (facility_id, time,
                        type_of_hospital,
                        ownership,
                        emergency,
                        if_float_for_str(overall_quality_rating)))
        except Exception as e:
            print("Insert failed!", e)
            df2.loc[nrow] = df.iloc[row, [0, 8, 9, 10, 12]]
            nrow += 1
        else:
            num_rows_inserted_quality += 1

print(nrow)
print(num_rows_inserted_quality)
df2.to_csv("failed_rows_cms.csv")