-- Table 1: hospital_info
-- The entity of this table is the basic information of the hospitals. 
-- We create a table for basic information for the hospitals in order to prevent redundancy  
-- in the database and to enhance the efficiency of the transactions. 
-- Table 2 and Table 3 will refer to Table 1 for hospital information. 


create table hospital_info (hospital_pk text primary key, state char(2), hospital_name text, address text unique, city text, zip char(5), fips_code integer 
check (fips_code > 0), geocoded_hospital_address point);


-- Table 2: hospital_weekly
-- The entity of this table is a weekly updated record of hospital information, 
-- which describes availability of the beds and occupancy of the patients. 
-- We create a table for weekly updates of the hospital operations in order to 
-- keep track of the changes in the status of each hospital. 


create table hospital_weekly (id serial primary key, hospital_pk text references hospital_info, collection_week date, all_adult_hospital_beds_7_day_avg numeric check (all_adult_hospital_beds_7_day_avg >= 0), 
all_pediatric_inpatient_beds_7_day_avg numeric check (all_pediatric_inpatient_beds_7_day_avg >= 0), all_adult_hospital_inpatient_bed_occupied_7_day_coverage numeric 
check (all_adult_hospital_inpatient_bed_occupied_7_day_coverage >=0), all_pediatric_inpatient_bed_occupied_7_day_avg numeric check (all_pediatric_inpatient_bed_occupied_7_day_avg >= 0), 
total_icu_beds_7_day_avg numeric check (total_icu_beds_7_day_avg >=0), icu_beds_used_7_day_avg numeric check (icu_beds_used_7_day_avg >= 0), inpatient_beds_used_covid_7_day_avg numeric check 
(inpatient_beds_used_covid_7_day_avg >= 0), staffed_adult_icu_patients_confirmed_covid_7_day_avg numeric check (staffed_adult_icu_patients_confirmed_covid_7_day_avg >= 0));


-- Table 3: hospital_quality
-- The entity of this table is the quality record of the hospitals, 
-- which describes the quality and also provides additional information 
-- related to the ownership, emergency status, and type of hospital.
-- We create a table for overall quality rating separately from the weekly 
-- updates of the hospital operations since the update frequency is 
-- different between the two data sets. 


create table hospital_quality (id serial primary key, facility_id text references hospital_info, update_time date check (update_time <= current_date), type_of_hospital text, ownership text, 
emergency text, overall_quality_rating numeric check (overall_quality_rating > 0));