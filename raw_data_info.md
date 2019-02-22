## Raw Data
Original data fromat is `csv` 
> medication_orders : this data is about order of medication per patient </br>
> lab_result : this data is about lab result of the patients </br>
> encounter : this data is about visit_details of the patients </br>
> encounter_dx : this data is containing visit code of the patients </br>


**medication_orders** </br>
 |-- Provider_Org: string (nullable = true) </br>
 |-- Member_ID: string (nullable = true) </br>
 |-- Last_Filled_Date: string (nullable = true) </br>
 |-- Drug_Name: string (nullable = true) </br>
 |-- Drug_NDC: string (nullable = true)</br>
 |-- Status: string (nullable = true)</br>
 |-- Sig: string (nullable = true)</br>
 |-- Route: string (nullable = true)</br>
 |-- Dose: string (nullable = true)</br>
 |-- Units: string (nullable = true)</br>
 |-- Order_ID: string (nullable = true)</br>
 |-- Order_Date: string (nullable = true)</br>
 |-- Qty_Ordered: string (nullable = true)</br>
 |-- Refills: string (nullable = true)</br>
 |-- Order_Provider_ID: string (nullable = true)</br>
 |-- Order_Provider_Name: string (nullable = true)</br>
 |-- Medication_Type: string (nullable = true)</br>
 |-- Encounter_ID: string (nullable = true)</br>
 
**lab_result** </br>
 |-- Provider_Org: string (nullable = true)</br>
 |-- Member_ID: string (nullable = true)</br>
 |-- Date_Collected: string (nullable = true)</br>
 |-- Test_ID: string (nullable = true)</br>
 |-- Specialty: string (nullable = true)</br>
 |-- Panel: string (nullable = true)</br>
 |-- Test_LOINC: string (nullable = true)</br>
 |-- Test_Name: string (nullable = true)</br>
 |-- Date_Resulted: string (nullable = true)</br>
 |-- Specimen: string (nullable = true)</br>
 |-- Result_LOINC: string (nullable = true)</br>
 |-- Result_Name: string (nullable = true)</br>
 |-- Result_Status: string (nullable = true)</br>
 |-- Result_Description: string (nullable = true)</br>
 |-- Numeric_Result: string (nullable = true)</br>
 |-- Units: string (nullable = true)</br>
 |-- Abnormal_Value: string (nullable = true)</br>
 |-- Reference_Range: string (nullable = true)</br>
 |-- Order_ID: string (nullable = true)</br>
 |-- Provider_ID: string (nullable = true)</br>
 |-- Encounter_ID: string (nullable = true)</br>
 
**encounter** </br>
Provider_Org
Encounter_ID
Member_ID
Provider_ID
Provider_NPI
Clinic_ID
Encounter_DateTime
Encounter_Description
CC
Episode_ID
Patient_DOB
Patient_Gender
Facility_Name
Provider_Name
Specialty
Clinic_Type
lab_orders_count
lab_results_count
medication_orders_count
medication_fulfillment_count
vital_sign_count
therapy_orders_count
therapy_actions_count
immunization_count
Has_Appt
SOAP_Note
consult_ordered	Disposition

**encounter_dx** </br>
|-- Provider_Org: string (nullable = true)</br>
|-- code: string (nullable = true)</br>
|-- vocab: string (nullable = true)</br>
|-- description: string (nullable = true)</br>
|-- severity: string (nullable = true)</br>
|-- Encounter_ID: string (nullable = true)</br>
