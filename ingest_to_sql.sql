CREATE DATABASE med_data_db;

USE med_data_db;

CREATE TABLE encounter_dx(
Provider_Org varchar(50) DEFAULT NULL,
code varchar(50) NOT NULL,
vocab varchar(50) DEFAULT NULL,
description varchar(200) DEFAULT NULL,
severity varchar(50) DEFAULT NULL,
Encounter_ID varchar(50) NOT NULL,
PRIMARY KEY(code, Encounter_ID)
);

CREATE TABLE encounter(
Provider_Org varchar(50) DEFAULT NULL,
Encounter_ID varchar(50) NOT NULL,
Member_ID varchar(50) DEFAULT NULL,
Provider_ID varchar(50) DEFAULT NULL,
Provider_NPI varchar(50) DEFAULT NULL,
Clinic_ID varchar(100) DEFAULT NULL,
Encounter_DateTime varchar(50) DEFAULT NULL,
Encounter_Description varchar(200) DEFAULT NULL,
CC varchar(100) DEFAULT NULL,
Episode_ID varchar(50) DEFAULT NULL,
Patient_DOB varchar(50) DEFAULT NULL,
Patient_Gender varchar(50) DEFAULT NULL,
Facility_Name varchar(50) DEFAULT NULL,
Provider_Name varchar(50) DEFAULT NULL,
Specialty varchar(50) DEFAULT NULL,
Clinic_Type varchar(50) DEFAULT NULL,
lab_orders_count varchar(50) DEFAULT NULL,
lab_results_count int(10) DEFAULT NULL,
medication_orders_count int(10) DEFAULT NULL,
medication_fulfillment_count int(10) DEFAULT NULL,
vital_sign_count int(10) DEFAULT NULL,
therapy_orders_count int(10) DEFAULT NULL,
therapy_actions_count int(10) DEFAULT NULL,
immunization_count int(10) DEFAULT NULL,
Has_Appt varchar(20) DEFAULT NULL,
SOAP_Note longtext DEFAULT NULL,
consult_ordered varchar(50) DEFAULT NULL,
Disposition varchar(50) DEFAULT NULL,
PRIMARY KEY(Encounter_ID)
);

CREATE TABLE lab(
Provider_Org varchar(50) DEFAULT NULL,
Member_ID varchar(50) DEFAULT NULL,
Date_Collected varchar(50) DEFAULT NULL,
Test_ID varchar(50) NOT NULL,
Specialty varchar(50) DEFAULT NULL,
Panel varchar(50) DEFAULT NULL,
Test_LOINC varchar(50) DEFAULT NULL,
Test_Name varchar(100) DEFAULT NULL,
Date_Resulted varchar(50) DEFAULT NULL,
Specimen varchar(50) DEFAULT NULL,
Result_LOINC varchar(50) NOT NULL,
Result_Name varchar(50) DEFAULT NULL,
Result_Status varchar(50) DEFAULT NULL,
Result_Description longtext DEFAULT NULL,
Numeric_Result varchar(50) DEFAULT NULL,
Units varchar(50) DEFAULT NULL,
Abnormal_Value varchar(50) DEFAULT NULL,
Reference_Range varchar(50) DEFAULT NULL,
Order_ID varchar(50) DEFAULT NULL,
Provider_ID varchar(50) DEFAULT NULL,
Encounter_ID varchar(50) NOT NULL,
PRIMARY KEY(Test_ID, Result_LOINC, Encounter_ID)
);

CREATE TABLE medication(
Provider_Org varchar(50) DEFAULT NULL,
Member_ID varchar(50) DEFAULT NULL,
Last_Filled_Date varchar(50) DEFAULT NULL,
Drug_Name varchar(50) DEFAULT NULL,
Drug_NDC varchar(50) NOT NULL,
Status varchar(50) DEFAULT NULL,
Sig varchar(50) DEFAULT NULL,
Route varchar(50) DEFAULT NULL,
Dose varchar(50) DEFAULT NULL,
Units varchar(50) DEFAULT NULL,
Order_ID varchar(50) DEFAULT NULL,
Order_Date varchar(50) DEFAULT NULL,
Qty_Ordered float(50) DEFAULT NULL,
Refills int(50) DEFAULT NULL,
Order_Provider_ID varchar(50) DEFAULT NULL,
Order_Provider_Name varchar(50) DEFAULT NULL,
Medication_Type varchar(50) DEFAULT NULL,
Encounter_ID varchar(50) NOT NULL,
PRIMARY KEY(Drug_NDC, Encounter_ID)
);

LOAD DATA LOCAL INFILE '/home/maria_dev/med_data/encounter_dx_INPUT.csv' 
INTO TABLE encounter_dx
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/home/maria_dev/med_data/encounter_INPUT.csv' 
INTO TABLE encounter
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/home/maria_dev/med_data/lab_results_INPUT.csv' 
INTO TABLE lab
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/home/maria_dev/med_data/medication_orders_INPUT.csv' 
INTO TABLE medication
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
