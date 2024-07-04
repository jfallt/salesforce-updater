from simple_salesforce import Salesforce, format_soql
import pandas as pd
import os
import yaml
from pathlib import Path

def get_settings():
    full_file_path = Path(__file__).parent.joinpath('settings.yaml')
    with open(full_file_path) as settings:
        settings_data = yaml.load(settings, Loader=yaml.Loader)
    return settings_data

config = get_settings()


# Funcs
def sql_where_convert(df_column):
    return ",".join([f"'{value}'" for value in df_column.to_list()])

# Auth
sf = Salesforce(
    version=config["API"]["version"],
    instance=config["API"]["base_url"] + config["API"]["version"],
    username=config["AUTH"]["username"],
    password=config["AUTH"]["password"],
    security_token=config["AUTH"]["security_token"],
)

def soql_to_df(query, column_aliases=None):
    print(query)
    df = pd.DataFrame(pd.DataFrame(sf.query(format_soql(query)))["records"].to_list())

    if "attributes" in df:
        df = df.drop(columns="attributes")
    if column_aliases:
       df.columns = column_aliases
    return df

# Load Data
path = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '../..', 'Documents'))
data_to_load = pd.read_excel(os.path.join(path, config["CONFIG"]["excel_sheet_name"]))

# [BEGIN Query Ids and Apply to Data]
print("Querying required IDs")
# Program_Term_Enrollment__c
#ProgramTermEnrollmentIDs = sql_where_convert(data_to_load["ProgramEnrollmentID"]) 
#program_enrollments = soql_to_df(f"SELECT Id, Name FROM hed__Program_Enrollment__c WHERE Name IN ({ProgramTermEnrollmentIDs})", ["Program_Enrollment__c", "Name"])
#data_to_load = data_to_load.merge(program_enrollments, left_on='ProgramEnrollmentID', right_on= "Name", how='left').drop(columns="Name")

# hed__Term__c
target_terms = sql_where_convert(data_to_load["Term"])
terms = soql_to_df(f"SELECT Id, Name FROM hed__Term__c WHERE Name IN ({target_terms})", ["Term__c", "Name"])
data_to_load = data_to_load.merge(terms, left_on='Term', right_on= "Name", how='left').drop(columns="Name")

# Students (Contact)
student_emails = sql_where_convert(data_to_load["Email"])
students = soql_to_df(f"SELECT Id, Active_Program_Enrollment__c, email FROM Contact WHERE email IN ({student_emails})", ["Student__c", "Program_Enrollment__c", "email_match"])
data_to_load = data_to_load.merge(students, left_on='Email', right_on= "email_match", how='left').drop(columns="email_match")
data_to_load["status"] = ""
print("IDs applied to data")
# [END Query Ids and Apply to Data]


# Data Update
for index, row in data_to_load.iterrows():
    print(f"Updating {row['LastName']}, {row['FirstName']}")

    # Payload for updated
    data = {
        'OwnerId': config["API"]["updater_id"], # this will always be the id from our config
        'Program_Enrollment__c': row["Program_Enrollment__c"],
        'Student__c': row["Student__c"], # Contact ID
        'Term__c': row["Term__c"],
        'Registration_Status__c': row["Registration Status"]
        }
    
    # Ensure a duplicate record is not created
    where_clause = ""
    for k, v in data.items():
        where_clause += f"{k}='{v}' AND "

    record_check = soql_to_df(f"SELECT Id, Name FROM Program_Term_Enrollment__c WHERE {where_clause[:-4]}", None)
    if not record_check.empty:
        print("A record already exists, skipping")
        data_to_load.loc[index, 'status'] = "Record already exists in Salesforce"
    else:
        response = sf.Program_Term_Enrollment__c.create(data)
        if response["success"]:
            print(f"Sucess! Updated {row['LastName']}, {row['FirstName']}")
            data_to_load.loc[index, 'status'] = "Success"
        else:
            print(f"Error Updating {row['LastName']}, {row['FirstName']}")
            data_to_load.loc[index, 'status'] = response["errors"]

# Write out results and purge the input file
# selecting rows based on condition 
df_success = data_to_load[data_to_load['status'] == "Success"] 
df_fail = data_to_load[data_to_load['status'] != "Success"]
df_success.to_excel(os.path.join(path, "ProgramTermEnrollmentUpdates_Success.xlsx"), index=False)
df_fail.to_excel(os.path.join(path, "ProgramTermEnrollmentUpdates_FAIL.xlsx"), index=False)

# Purge Input File