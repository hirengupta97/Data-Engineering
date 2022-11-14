"""Implement a script to load quality data in the database"""

import psycopg
import pandas as pd
import sys


conn = psycopg.connect(
    host = "sculptor.stat.cmu.edu", dbname = "hireng",
    user = "hireng", password = ""
)

cur = conn.cursor()

file_name = sys.argv[3]
date = sys.argv[2]

conn.autocommit = True

num_rows_inserted_quality = 0

# make a new transaction
with conn.transaction():
    for row in file_name:
        try:
            # make a new SAVEPOINT -- like a save in a video game
            with conn.transaction():
                # perhaps a bunch of reformatting and data manipulation goes here

                facility_id, type_of_hospital, ownership, emergency, overall_quality_rating = row
                # now insert the data
                cur.execute("insert into hospital_quality(facility_id, update_time, type_of_hospital, ownership, emergency, overall_quality_rating) " 
                            "values (%s, %s, %s, %s, %s, %s)", (facility_id, date, type_of_hospital, ownership, emergency, overall_quality_rating))
        except Exception as e:
            # if an exception/error happens in this block, Postgres goes back to
            # the last savepoint upon exiting the `with` block
            print("insert failed")
            # add additional logging, error handling here
        else:
            # no exception happened, so we continue without reverting the savepoint
            num_rows_inserted_quality += 1

# now we commit the entire transaction
conn.commit()
