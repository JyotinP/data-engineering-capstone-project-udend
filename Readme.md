### Step 3: Define the Data Model
# 3.1 Conceptual Data Model
### Map out the conceptual data model and explain why you chose that model

- We chose this model so the code data from the Fact immigration table can be joined with the Dimension tables. For e.g 'ctry_of_res_code' column of fact_immigration Fact table can be joined with 'country_code' column of dim_country Dimension table. 
- Similarly 'dest_state_code' column of fact_immigration Fact table can be joined with 'state_code' column of dim_demographics Dimension table.

fact_immigration
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- ctry_of_res_code: integer (nullable = true)
 |-- port_code: string (nullable = true)
 |-- mode_code: integer (nullable = true)
 |-- dest_state_code: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- visa_code: integer (nullable = true)
 |-- dob: integer (nullable = true)
 |-- gender: string (nullable = true)
 |-- airline: string (nullable = true)
 |-- visa_type: string (nullable = true)
 |-- arrvl_dt: date (nullable = true)
 |-- depart_dt: date (nullable = true)

dim_date
 |-- arrvl_dt: date (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- week_of_year: integer (nullable = true)
 |-- day_of_year: integer (nullable = true)
 |-- day_of_month: integer (nullable = true)
 |-- day_of_week: integer (nullable = true)
 |-- quarter: integer (nullable = true)

dim_demographics
 |-- state_code: string (nullable = true)
 |-- medn_age: float (nullable = true)
 |-- male_pop: integer (nullable = true)
 |-- female_pop: integer (nullable = true)
 |-- total_pop: integer (nullable = true)
 |-- vet_cnt: integer (nullable = true)
 |-- foreign_born: integer (nullable = true)
 |-- avg_hsehld_size: float (nullable = true)

dim_state
 |-- state_code: string (nullable = true)
 |-- state_name: string (nullable = true)

dim_country
 |-- country_code: integer (nullable = true)
 |-- country_name: string (nullable = true)

dim_port
 |-- port_code: string (nullable = true)
 |-- port_name: string (nullable = true)

dim_mode
 |-- mode_code: integer (nullable = true)
 |-- mode_name: string (nullable = true)

dim_visa
 |-- visa_code: integer (nullable = true)
 |-- visa_name: string (nullable = true)



# 4.3 Data dictionary 
## Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.

fact_immigration:
 |-- year: 4 digit year
 |-- month: Numeric month
 |-- ctry_of_res_code: Country of residence code
 |-- port_code: Port of entry code
 |-- mode_code: Mode of transport code
 |-- dest_state_code: Destination state code
 |-- age: Age of Respondent in Years
 |-- visa_code: Visa codes collapsed into three categories
 |-- dob: 4 digit year of birth
 |-- gender: Non-immigrant sex
 |-- airline: Airline used to arrive in U.S.
 |-- visa_type: Class of admission legally admitting the non-immigrant to temporarily stay in U.S.
 |-- arrvl_dt: Arrival Date in the USA
 |-- depart_dt: Departure Date from the USA
 
 
 dim_demographics
 |-- state_code: Two letter state code
 |-- medn_age: Median age
 |-- male_pop: Male population
 |-- female_pop: Female population
 |-- total_pop: Total population
 |-- vet_cnt: Number of Veterans
 |-- foreign_born: Number of foreign-born
 |-- avg_hsehld_size: Average Household Size
 
 
 dim_state
 |-- state_code: (I94ADDR) Two letter state code from I94_SAS_Labels_Descriptions
 |-- state_name: State name
 
 dim_country
 |-- country_code: (I94RES) Numeric country code from I94_SAS_Labels_Descriptions
 |-- country_name: Country of residence
 
 dim_port
 |-- port_code: (I94PORT) Port of entry code from I94_SAS_Labels_Descriptions
 |-- port_name: Port of entry
 
 dim_mode
 |-- mode_code: (I94MODE) Mode of transport code from I94_SAS_Labels_Descriptions
 |-- mode_name: Mode of transport
 
 dim_visa
 |-- visa_code: Visa codes collapsed into three categories from I94_SAS_Labels_Descriptions
 |-- visa_name: Visa purpose of visit
 
dim_date
 |-- arrvl_dt: Arrival Date in the USA
 |-- year: Year
 |-- month: Month
 |-- week_of_year: Week of the year
 |-- day_of_year: Day of the year
 |-- day_of_month: Day of the month
 |-- day_of_week: Day of the week
 |-- quarter: Quarter