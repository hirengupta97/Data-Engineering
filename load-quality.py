"""Implement a script to load quality data in the database"""

import psycopg
import pandas as pd
import numpy as np
import sys

# Connect to the sculptor
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname="hireng",
    user="hireng", password="aiVee0Ohs"
)
cur = conn.cursor()

# Acquire file name from the command line (3rd element)
file_name = sys.argv[2]
# Acquire date of the file from the command line (2nd element)
date = sys.argv[1]

# Identify -999999 and Nan as missing values
missing_values = [-999999, np.nan]
# Make dataframe from the imported .csv file
# affirm that pre-identified missing values (-999999 and Nan)
# as na_values within the dataframe
df = pd.read_csv(file_name, na_values=missing_values)
# If the value is "Not Available" in the column "Hospital overall rating"
# convert into Nan value
df.loc[df["Hospital overall rating"] ==
       "Not Available", "Hospital overall rating"] = np.nan

# Enable auto commit
conn.autocommit = True


# Create a function that transforms string value to float
# to quantify hospital overall rating
# return None otherwise
def if_float_for_str(n):
    if (type(n) == str):
        return float(n)
    else:
        return None


# Create an empty dataframe that will store the rows
# for which values fail to be inserted into the database
df2 = pd.DataFrame(columns=[
    "hospital_pk", "type_of_hospital",
    "ownership", "emergency", "overall_quality_rating"])

# Instantiate a varible that counts the number of rows
# for which values fail to be inserted into the database
nrow = 0
# Instantiate varibles that counts the number rows
# inserted into pre-defined SQL schema
# (hospital_quality)
num_rows_inserted_quality = 0

# Insert each row of the dataset to SQL table
# Connect to SQL and make transaction
with conn.transaction():
    # Perform for-loop execution for every row in the dataframe
    for row in range(df.shape[0]):
        # Identify each column in the dataframe
        # (as pre-defined from SQL schema)
        hospital_pk = df.iloc[row, 0]
        time = date
        type_of_hospital = df.iloc[row, 8]
        ownership = df.iloc[row, 9]
        emergency = df.iloc[row, 10]
        overall_quality_rating = df.iloc[row, 12]
        # Call try-except so that we can store
        # fail-to-upload values into df2
        try:
            with conn.transaction():
                cur.execute(
                    # Insert pre-identified values for each variable into SQL
                    # for each row in the dataframe
                    "insert into hospital_quality"
                    "(hospital_pk, update_time, type_of_hospital,"
                    "ownership, emergency, overall_quality_rating)"
                    "values ( %s, %s, %s, %s, %s, %s)",
                    (hospital_pk, time,
                        type_of_hospital,
                        ownership,
                        emergency,
                        if_float_for_str(overall_quality_rating)))
        except Exception as e:
            # If the execution fails, store the fail-to-insert row (entire row)
            # into the dataframe df2
            print("Insert failed!", e)
            df2.loc[nrow] = list(df.iloc[row, [0, 8, 9, 10, 12]])
            # Add a count to the number of rows
            # for which values fail to be inserted into the database
            nrow += 1
        else:
            # If properly added to the SQL table,
            # add a count to the number of rows updated
            # for hospital_quality
            num_rows_inserted_quality += 1

# Print number of rows where updates failed
print(nrow)
# Print the number of rows where updates succeeded
print(num_rows_inserted_quality)
# Export the collection fail-to-insert data in CMS_data
# (dataframe, df2) as a .csv file
df2.to_csv("failed_rows_cms.csv")
