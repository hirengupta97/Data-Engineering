"""Implementation fro SQL tables to analyze the data sets"""


import psycopg
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate
import warnings
import plotly.express as px
import streamlit as st


warnings.filterwarnings("ignore")
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname="ruilinw",
    user="ruilinw", password="Eip8oosei"
)
cur = conn.cursor()

# d = pd.read_sql_query("SELECT collection_week, count(*)"
#                       "FROM hospital_weekly "
#                       "WHERE collection_week = (SELECT max(collection_week) "
#                       "FROM hospital_weekly) "
#                       "GROUP BY collection_week;",
#                       conn)

# Q1 A summary of how many hospital records were loaded in the most recent
# week, and how that compares to previous weeks.

records = pd.read_sql_query("SELECT collection_week, count(*) AS count "
                            "FROM hospital_weekly "
                            "GROUP BY collection_week "
                            "ORDER BY collection_week desc ",
                            conn)

# Q2 A table summarizing the number of adult and pediatric beds available
# this week, the number used, and the number used by patients with COVID,
# compared to the 4 most recent weeks

beds = pd.read_sql_query("SELECT collection_week, "
                         "sum(all_adult_hospital_beds_7_day_avg) "
                         "AS adult_beds_available, "
                         "sum(all_pediatric_inpatient_beds_7_day_avg) "
                         "AS pediatric_beds_available, "
                         "sum(all_adult_hospital_inpatient_bed_"
                         "occupied_7_day_coverage) "
                         "AS adult_beds_occupied, "
                         "sum(all_pediatric_inpatient_bed_occupied_7_day_avg) "
                         "AS pediatric_beds_occupied, "
                         "sum(inpatient_beds_used_covid_7_day_avg) "
                         "AS inpatient_beds_used_covid "
                         "FROM hospital_weekly "
                         "GROUP BY collection_week; ",
                         conn)


# Q3 A graph or table summarizing the fraction of beds currently in use by
# hospital quality rating, so we can compare high-quality
# and low-quality hospitals

quality = pd.read_sql_query("SELECT overall_quality_rating, "
                            "sum(bed_in_use) AS bed_in_use "
                            "FROM (SELECT hospital_pk, "
                            "sum(all_adult_hospital_inpatient_bed_occupied_"
                            "7_day_coverage) + sum(all_pediatric_inpatient_"
                            "bed_occupied_7_day_avg) AS bed_in_use "
                            "FROM hospital_weekly "
                            "WHERE collection_week = "
                            "(SELECT max(collection_week) "
                            "FROM hospital_weekly) "
                            "GROUP BY hospital_pk) A "
                            "JOIN (SELECT hospital_pk, overall_quality_rating "
                            "FROM hospital_quality) B ON A.hospital_pk = "
                            "B.hospital_pk "
                            "GROUP BY overall_quality_rating "
                            "ORDER BY overall_quality_rating ASC;",
                            conn)

# Q4 A plot of the total number of hospital beds used per week, over all time,
# split into all cases and COVID cases

covid = pd.read_sql_query("SELECT collection_week, "
                          "sum(all_adult_hospital_inpatient_bed_occupied_"
                          "7_day_coverage) + sum(all_pediatric_inpatient_bed_"
                          "occupied_7_day_avg) AS bed_in_use_all_cases,"
                          "sum(inpatient_beds_used_covid_7_day_avg) AS "
                          "bed_in_use_covid "
                          "FROM hospital_weekly "
                          "GROUP BY collection_week "
                          "ORDER BY collection_week ASC;",
                          conn)

# Q5 A map showing the number of COVID cases by state (the first two digits of
# a hospital ZIP code is its state)

map_1 = pd.read_sql_query("SELECT state, sum(covid_cases) "
                          "AS total_covid_cases "
                          "FROM (SELECT hospital_pk, state "
                          "FROM hospital_info) A "
                          "JOIN (SELECT hospital_pk, "
                          "sum(inpatient_beds_used_covid_7_day_avg) "
                          "AS covid_cases "
                          "FROM hospital_weekly "
                          "WHERE collection_week = (SELECT "
                          "max(collection_week) "
                          "FROM hospital_weekly) "
                          "GROUP BY hospital_pk) B "
                          "ON A.hospital_pk = B.hospital_pk "
                          "GROUP BY state; ",
                          conn)

# Q6 A table of the states in which the number of cases has increased
# by the most since last week

covid_2 = pd.read_sql_query("SELECT state, covid_cases_ct, covid_cases_lw "
                            "FROM (SELECT state, sum("
                            "CASE WHEN collection_week = "
                            "(SELECT max(collection_week) "
                            "FROM hospital_weekly) "
                            "THEN inpatient_beds_used_covid_7_day_avg "
                            "END) AS covid_cases_ct, sum("
                            "CASE WHEN collection_week = "
                            "(SELECT max(collection_week) "
                            "FROM hospital_weekly "
                            "WHERE collection_week < ("
                            "SELECT max(collection_week) "
                            "FROM hospital_weekly)) "
                            "THEN inpatient_beds_used_covid_7_day_avg "
                            "END) AS covid_cases_lw "
                            "FROM (SELECT hospital_pk, state "
                            "FROM hospital_info) A "
                            "JOIN (SELECT hospital_pk,collection_week, "
                            "inpatient_beds_used_covid_7_day_avg "
                            "FROM hospital_weekly) B "
                            "ON A.hospital_pk = B.hospital_pk "
                            "GROUP BY state) AS C "
                            "WHERE covid_cases_ct > covid_cases_lw;",
                            conn)

# A table of the hospitals (including names and locations) with the
# largest changes in COVID cases in the last week

covid_3 = pd.read_sql_query("SELECT hospital_name, address, "
                            "abs(covid_cases_ct - covid_cases_lw) AS diff "
                            "FROM (SELECT A.hospital_pk, "
                            "address, hospital_name, sum("
                            "CASE WHEN collection_week = ("
                            "SELECT max(collection_week) "
                            "FROM hospital_weekly) "
                            "THEN inpatient_beds_used_covid_7_day_avg "
                            "END) AS covid_cases_ct, sum("
                            "CASE WHEN collection_week = ("
                            "SELECT max(collection_week) "
                            "FROM hospital_weekly "
                            "WHERE collection_week < "
                            "(SELECT max(collection_week) "
                            "FROM hospital_weekly)) "
                            "THEN inpatient_beds_used_covid_7_day_avg "
                            "END) AS covid_cases_lw "
                            "FROM (SELECT hospital_pk, hospital_name, address "
                            "FROM hospital_info) A "
                            "JOIN (SELECT hospital_pk,collection_week, "
                            "inpatient_beds_used_covid_7_day_avg "
                            "FROM hospital_weekly) B "
                            "ON A.hospital_pk = B.hospital_pk "
                            "GROUP BY A.hospital_pk, A.hospital_name, "
                            "A.address) AS C "
                            "WHERE covid_cases_ct IS NOT NULL "
                            "AND covid_cases_lw IS NOT NULL "
                            "ORDER BY diff DESC "
                            "LIMIT 10;",
                            conn)

# Setting the index from 1
records = records.set_index(records.index + 1)
beds = beds.set_index(beds.index + 1)
quality = quality.set_index(quality.index + 1)
covid = covid.set_index(covid.index + 1)
map_1 = map_1.set_index(map_1.index + 1)
covid_2 = covid_2.set_index(covid_2.index + 1)
covid_3 = covid_3.set_index(covid_3.index + 1)

# Implement streamlit
st.title("Hospital Operation Analysis Weekly Report")

fig, ax = plt.subplots()

# Plot for question 1
plot_1 = records.plot(ax=ax,
                      kind="bar",
                      x="collection_week",
                      y="count",
                      xlabel="Collection Weeks",
                      ylabel="Count")

# Table for question 1
st.table(records)
# st.write(tabulate(lw_date, headers=lw_date.columns, tablefmt='simple'))

# Annotation for plot of question 1
for bar in plot_1.patches:
    plt.annotate(format(bar.get_height()),
                 (bar.get_x() + bar.get_width() / 2,
                 bar.get_height()), ha='center', va='center',
                 size=10, xytext=(0, 8),
                 textcoords='offset points')

ax.get_legend().remove()


st.pyplot(fig)
# plt.show()

# Plot for question 2
plot_2 = beds.plot(kind="bar",
                   x="collection_week",
                   xlabel="Beds Status",
                   ylabel="Total")
# Table for question 2
print(tabulate(beds, headers=beds.columns, tablefmt='psql'))

# Annotation for plot of question 2
for bar in plot_2.patches:
    plt.annotate(format(bar.get_height()),
                 (bar.get_x() + bar.get_width() / 2,
                 bar.get_height()), ha='center', va='center',
                 size=10, xytext=(0, 8),
                 textcoords='offset points')

plt.legend(loc='best', bbox_to_anchor=(0.5, 0., 0.5, 0.5))
plt.show()

# Plot for question 3
plot_3 = quality.plot(kind="bar",
                      x="overall_quality_rating",
                      xlabel="Ratings",
                      ylabel="Beds in Use")

# Table for question 3
print(tabulate(quality, headers=quality.columns,
               tablefmt='psql'))

# Annotation for plot of question 3
for bar in plot_3.patches:
    plt.annotate(format(bar.get_height()),
                 (bar.get_x() + bar.get_width() / 2,
                 bar.get_height()), ha='center', va='center',
                 size=10, xytext=(0, 8),
                 textcoords='offset points')

plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),
           ncol=3, fancybox=True, shadow=True)
plt.show()

# Plot for question 4
plot_4 = covid.plot(kind="bar",
                    x="collection_week",
                    xlabel="Total",
                    ylabel="Beds in Use")

# Table for question 4
print(tabulate(covid, headers=covid.columns,
               tablefmt='psql'))

# Annotation for plot of question 4
for bar in plot_4.patches:
    plt.annotate(format(bar.get_height()),
                 (bar.get_x() + bar.get_width() / 2,
                 bar.get_height()), ha='center', va='center',
                 size=10, xytext=(0, 8),
                 textcoords='offset points')

plt.legend(loc='best')
plt.show()

# Plot for question 5
plot_5 = px.choropleth(map_1,
                       locations='state',
                       locationmode="USA-states",
                       scope="usa",
                       color='total_covid_cases',
                       color_continuous_scale="Viridis_r",
                       )
plot_5.show()

# Table for queation 5
print(tabulate(map_1, headers=map_1.columns, tablefmt='psql'))

# Table for queation 6
print(tabulate(covid_2, headers=covid_2.columns, tablefmt='psql'))

# Table for queation 7
print(tabulate(covid_3, headers=covid_3.columns, tablefmt='psql'))
