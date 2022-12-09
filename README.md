# Data-Engineering
We will be developing a data pipeline for the unstructured and messy data that has been provided to produce a structured SQL database, and automate the data updates and generate efficient and meaningful automated reports. The data provided will be from the US Department of Health and Human Services (HHS) about hospitals functioning in the country. The data includes over a hundred variables, we will be focusing at roughly thirty variables for our database and analysis through automated reports.

Specifically, in this Github there are three main components: schema.sql, load-hhs.py, and load-quality.py. 

### Steps to generate weekly reports
1. Run load-hhs.py file with argument of the name of the csv file in terminal to load HHS data for each week:
   "python load-hhs.py 2022-01-04-hhs-data.csv"
2. Run load-quality.py file with arguments of date of the records and name of csv file in terminal to load CMS data for each month:
   "python load-quality.py 2021-07-01 Hospital_General_Information-2021-07.csv"
3. Run analytics.py file with argument of date for weekly report in terminal to generate the report in streamlit interactive platform:
   "python analytics.py 2022-09-30"

The detailed information for the files in this Github is as follows: 

### schema.sql
In this file, we identified three schemas - hospital_info, hospital_weekly, and hospital_quality. Each schema works as follows:

Table 1: Hospital_info: a table that stores permanent information about the hospitals 

Table 2: Hospital_weekly: a table that stores weekly updates regarding the hospital operation (these features will be updated weekly)

Table 3: Hospital_quality: a table that stores overall quality rating (these features will be updated several times a year)

### load-hhs.py
In this file, we create two execution functions. 

For table1: Update the table hospital_info, which only stores permanent information about the hospitals. Since these are permanent information they should stay the same when the weekly updates take place. Hence, when there is any conflict (i.e. when duplicates exist), we just ignore the row and proceed to the next row to store the information (by using ON CONFLICT DO NOTHING).

For table2:  Upate the table hospital_weekly, which stores weekly updated information about the hospitals. Since these are changing information, instead of ON CONFLICT DO NOTHING, we perform try-except clause, for which allows us to take an action (e.g. store the fail-to-insert data into a different dataframe) in the incident of any failure.

(Please check individual files for specific comments.)

We begin with data cleaning: we replace Nan values to None, which allows Python to recognize None = null values for our qunatitative analysis later. Then, we insert each row of the dataset to SQL table using SQL INSERT VALUE INTO query. In order to perform this, we need to perform for-loop execution for every row in the dataframe, identify each column in the dataframe (as pre-defined from SQL schema), and insert those values into the appropriate variable location within the schema. 


### load-quality.py

In this file, we create one execution functions. 

For table3: Upate the table hospital_quality, which stores irregular updates about hospital quality information. Since these are also changing information, instead of ON CONFLICT DO NOTHING, we perform try-except clause, for which allows us to take an action (e.g. store the fail-to-insert data into a different dataframe) in the incident of any failure.

(Please check individual files for specific comments.)

Once again, we begin with data cleaning: we replace Nan values to None, which allows Python to recognize None = null values for our qunatitative analysis later. Then, we insert each row of the dataset to SQL table using SQL INSERT VALUE INTO query. In order to perform this, we need to perform for-loop execution for every row in the dataframe, identify each column in the dataframe (as pre-defined from SQL schema), and insert those values into the appropriate variable location within the schema. 
