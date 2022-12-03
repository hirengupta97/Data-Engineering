"""Implementation fro SQL tables to analyze the data sets"""


import psycopg
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate
import warnings
import plotly.express as px
# import streamlit as st


warnings.filterwarnings("ignore")
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname="hireng",
    user="hireng", password="aiVee0Ohs"
)
cur = conn.cursor()

d = pd.read_sql_query("select collection_week, count(*) from hospital_weekly "
                      "where collection_week = (select max(collection_week) "
                      "from hospital_weekly) group by collection_week;", conn)


lw_date = pd.read_sql_query("select collection_week, count(*) as count "
                            "from hospital_weekly group by collection_week "
                            "order by collection_week desc",
                            conn)


beds = pd.read_sql_query("select collection_week, "
                         "sum(all_adult_hospital_beds_7_day_avg)"
                         " AS adult_beds_available, "
                         "sum(all_pediatric_inpatient_beds_7_day_avg) "
                         "AS pediatric_beds_available, "
                         "sum(all_adult_hospital_inpatient_bed_"
                         "occupied_7_day_coverage) "
                         "AS adult_beds_occupied, "
                         "sum(all_pediatric_inpatient_bed_occupied_7_day_avg) "
                         "AS pediatric_beds_occupied, "
                         "sum(inpatient_beds_used_covid_7_day_avg) AS "
                         "inpatient_beds_used_covid from hospital_weekly "
                         "group by collection_week; ", conn)

quality = pd.read_sql_query("select overall_quality_rating, sum(bed_in_use) "
                            "AS bed_in_use from (select hospital_pk, "
                            "sum(all_adult_hospital_inpatient_bed_occupied_"
                            "7_day_coverage) + sum(all_pediatric_inpatient_"
                            "bed_occupied_7_day_avg) AS bed_in_use "
                            "from hospital_weekly where collection_week"
                            "=(select max(collection_week)"
                            " from hospital_weekly) group by hospital_pk) A "
                            "join (select hospital_pk, overall_quality_rating "
                            "from hospital_quality) B on A.hospital_pk"
                            "=B.hospital_pk group by overall_quality_rating "
                            "order by overall_quality_rating asc;", conn)

covid = pd.read_sql_query("select collection_week, "
                          "sum(all_adult_hospital_inpatient_bed_occupied_"
                          "7_day_coverage) + sum(all_pediatric_inpatient_bed_"
                          "occupied_7_day_avg) AS bed_in_use_all_cases,"
                          "sum(inpatient_beds_used_covid_7_day_avg) AS "
                          "bed_in_use_covid from hospital_weekly group by "
                          "collection_week order by collection_week asc;",
                          conn)

df = pd.read_sql_query(
        "select state, sum(covid_cases) as total_covid_cases from "
        "(select hospital_pk, state from hospital_info)A join "
        "(select hospital_pk, sum(inpatient_beds_used_covid_7_day_avg)"
        " AS covid_cases from hospital_weekly where collection_week=(select "
        "max(collection_week) from hospital_weekly) group by "
        "hospital_pk) B on A.hospital_pk=B.hospital_pk group by "
        "state; ", conn)

df5 = pd.read_sql_query(
    "select state, covid_cases_ct, covid_cases_lw "
    "from (select state, sum(case when collection_week = (select "
    "max(collection_week) from hospital_weekly) then "
    "inpatient_beds_used_covid_7_day_avg end) as covid_cases_ct,"
    "sum(case when collection_week = (select max(collection_week) "
    "from hospital_weekly where collection_week<(select max(collection_week) "
    "from hospital_weekly)) then inpatient_beds_used_covid_7_day_avg end) "
    "as covid_cases_lw from (Select hospital_pk, state from hospital_info)A "
    "Join (select hospital_pk,collection_week, "
    "inpatient_beds_used_covid_7_day_avg from hospital_weekly)B on "
    "A.hospital_pk=B.hospital_pk Group by state) AS C where "
    "covid_cases_ct>covid_cases_lw;", conn)

df6 = pd.read_sql_query(
    "select hospital_name, address, abs(covid_cases_ct - covid_cases_lw) as "
    "diff from (select A.hospital_pk, address, hospital_name, sum(case when "
    "collection_week = (select max(collection_week) from hospital_weekly) then"
    " inpatient_beds_used_covid_7_day_avg end) as covid_cases_ct,"
    "sum(case when collection_week = (select max(collection_week) "
    "from hospital_weekly where collection_week<(select max(collection_week) "
    "from hospital_weekly)) then inpatient_beds_used_covid_7_day_avg end) "
    "as covid_cases_lw from (Select hospital_pk, hospital_name, address from "
    "hospital_info)A Join (select hospital_pk,collection_week, "
    "inpatient_beds_used_covid_7_day_avg from hospital_weekly)B on "
    "A.hospital_pk=B.hospital_pk group by A.hospital_pk, A.hospital_name, "
    "A.address) AS C where covid_cases_ct is not null and covid_cases_lw "
    "is not null order by diff desc limit 10;", conn)

lw_date = lw_date.set_index(lw_date.index + 1)
beds = beds.set_index(beds.index + 1)
quality = quality.set_index(quality.index + 1)
covid = covid.set_index(covid.index + 1)
df = df.set_index(df.index + 1)
df5 = df5.set_index(df5.index + 1)
df6 = df6.set_index(df6.index + 1)

plots = lw_date.plot(kind="bar", x="collection_week",
                     y="count", xlabel="Collection Weeks", ylabel="Count")

print(tabulate(lw_date, headers=lw_date.columns, tablefmt='psql'))

for bar in plots.patches:
    plt.annotate(format(bar.get_height()),
                 (bar.get_x() + bar.get_width() / 2,
                 bar.get_height()), ha='center', va='center',
                 size=10, xytext=(0, 8),
                 textcoords='offset points')


plt.show()


plots1 = beds.plot(kind="bar", x="collection_week", xlabel="Beds Status",
                   ylabel="Total")

print(tabulate(beds, headers=beds.columns, tablefmt='psql'))

for bar in plots1.patches:
    plt.annotate(format(bar.get_height()),
                 (bar.get_x() + bar.get_width() / 2,
                 bar.get_height()), ha='center', va='center',
                 size=10, xytext=(0, 8),
                 textcoords='offset points')

plt.legend(loc='best', bbox_to_anchor=(0.5, 0., 0.5, 0.5))
plt.show()


plots2 = quality.plot(kind="bar", x="overall_quality_rating",
                      xlabel="Ratings", ylabel="Beds in Use")

print(tabulate(quality, headers=quality.columns,
               tablefmt='psql'))

for bar in plots2.patches:
    plt.annotate(format(bar.get_height()),
                 (bar.get_x() + bar.get_width() / 2,
                 bar.get_height()), ha='center', va='center',
                 size=10, xytext=(0, 8),
                 textcoords='offset points')

plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),
           ncol=3, fancybox=True, shadow=True)
plt.show()

plots3 = covid.plot(kind="bar", x="collection_week",
                    xlabel="Total", ylabel="Beds in Use")

print(tabulate(covid, headers=covid.columns,
               tablefmt='psql'))

for bar in plots3.patches:
    plt.annotate(format(bar.get_height()),
                 (bar.get_x() + bar.get_width() / 2,
                 bar.get_height()), ha='center', va='center',
                 size=10, xytext=(0, 8),
                 textcoords='offset points')

plt.legend(loc='best')
plt.show()

fig = px.choropleth(df,
                    locations='state',
                    locationmode="USA-states",
                    scope="usa",
                    color='total_covid_cases',
                    color_continuous_scale="Viridis_r",
                    )
fig.show()

print(tabulate(df, headers=df.columns, tablefmt='psql'))
print(tabulate(df5, headers=df5.columns, tablefmt='psql'))
print(tabulate(df6, headers=df6.columns, tablefmt='psql'))
