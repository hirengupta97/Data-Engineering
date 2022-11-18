"""Implement a script to load HHS data in the database"""

import psycopg
import pandas as pd
import numpy as np
import math as m
import sys

# Connect to the sculptor
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname="dshim",
    user="dshim", password="raiXa2ahd"
)
cur = conn.cursor()

# Acquire file name from the command line (2nd element)
file_name = sys.argv[1]

# Make dataframe from the imported .csv file
df = pd.read_csv(file_name)
# Replace Nan values to None
# (this allows Python to recognize None = null values)
df = df.replace({np.nan: None})

# Enable auto commit
conn.autocommit = True

# Insert each row of the dataset to SQL table
# Connect to SQL and make transaction
with conn.transaction():
    # Perform for-loop execution for every row in the dataframe
    for row in range(df.shape[0]):
        # Identify each column in the dataframe
        # (as pre-defined from SQL schema)
        hospital_pk = df.iloc[row, 0]
        state = df.iloc[row, 2]
        hospital_name = df.iloc[row, 4]
        address = df.iloc[row, 5]
        city = df.iloc[row, 6]
        zip = df.iloc[row, 7]
        fips_code = df.iloc[row, 9]
        geocoded = (df.iloc[row, 96])
        # Since geocoded constitues of 2 variables, logitude and latitude,
        # split the values into 2 different components (logitude and latitude)
        # if value is not None
        if (((geocoded is None))):
            longitude = None
            latitude = None
        else:
            longitude = float(geocoded[6:].strip("()").split(" ")[0])
            latitude = float(geocoded[6:].strip("()").split(" ")[1])
        # Execute SQL query
        # If ON CONFLICT (if hospital data already exists),
        # DO NOTHING (we do not add any further information in this case)
        cur.execute(
            # Insert the pre-identified values for each variable into SQL table
            # for each row in the dataframe
            "insert into hospital_info (hospital_pk, state, hospital_name,"
            "address, city, zip, fips_code, longitude, latitude)"
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            "ON CONFLICT (hospital_pk) DO NOTHING ",
            (hospital_pk,
                state,
                hospital_name,
                address, city,
                int(zip),
                fips_code,
                longitude,
                latitude))

# Instantiate varibles that counts the number rows
# inserted into pre-defined SQL schema
# (hospital_info)
num_rows_inserted_info = 0

# Identify -999999 and Nan as missing values
missing_values = [-999999, np.nan]
# Make dataframe from the imported .csv file
# affirm that pre-identified missing values (-999999 and Nan)
# as na_values within the dataframe
df = pd.read_csv(file_name, na_values=missing_values)


# Create a function that transforms Nan value to None
# (this allows Python to recognize None = null values)
# Cast the value "n" as float otherwise
def if_float(n):
    if (m.isnan(n)):
        return None
    else:
        return float(n)


# Instantiate a varible that counts the number of rows
# for which values fail to be inserted into the database
nrow = 0

# Create an empty dataframe that will store the rows
# for which values fail to be inserted into the database
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

# Instantiate varibles that counts the number rows
# inserted into pre-defined SQL schema
# (hospital_weekly)
num_rows_inserted_weekly = 0

# Insert each row of the dataset to SQL table
# Connect to SQL and make transaction
with conn.transaction():
    # Perform for-loop execution for every row in the dataframe
    for row in range(df.shape[0]):
        # Identify each column in the dataframe
        # (as pre-defined from SQL schema)
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
        # Call try-except so that we can store
        # fail-to-upload values into df2
        try:
            with conn.transaction():
                cur.execute(
                    # Insert pre-identified values for each variable into SQL
                    # for each row in the dataframe
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
            # If the execution fails, store the fail-to-insert row (entire row)
            # into the dataframe df2
            print("Insert failed!", e)
            df2.loc[nrow] = list(df.iloc[
                row, [0, 1, 12, 111, 55, 108, 22, 24, 16, 27]])
            # Add a count to the number of rows
            # for which values fail to be inserted into the database
            nrow += 1
        else:
            # If properly added to the SQL table,
            # add a count to the number of rows updated
            # for hospital_weekly
            num_rows_inserted_weekly += 1

# Print number of rows where updates failed
print(nrow)
# Print the number of rows where updates succeeded
print(num_rows_inserted_weekly)
# Export the collection fail-to-insert data
# (dataframe, df2) as a .csv file
df2.to_csv("failed_rows_hhs.csv")
