from merrimack import sql_where_convert, soql_to_df, load_input_file, get_contact_id, config, sf, write_results

# Load Data
data_to_load = load_input_file("program_term_enrollment_updates")
print(f"Updating program enrollments for {len(data_to_load.index)} student(s)\n")

# [BEGIN Query Ids and Apply to Data]
print("Querying required IDs\n")

# hed__Term__c
target_terms = sql_where_convert(data_to_load["Term"])
terms = soql_to_df(
    f"SELECT Id, Name FROM hed__Term__c WHERE Name IN ({target_terms})",
    ["Term__c", "Name"],
)
data_to_load = data_to_load.merge(
    terms, left_on="Term", right_on="Name", how="left"
).drop(columns="Name")

# Students (Contact)
data_to_load = get_contact_id(data_to_load)
print("IDs applied to data")
# [END Query Ids and Apply to Data]


# Data Update
data_to_load["status"] = ""
print("Updating Data\n")
for index, row in data_to_load.iterrows():
    print(row)

    # Payload for updated
    data = {
        "OwnerId": config["API"][
            "updater_id"
        ],  # this will always be the id from our config
        "Program_Enrollment__c": row["Program_Enrollment__c"],
        "Student__c": row["Student__c"],  # Contact ID
        "Term__c": row["Term__c"],
        "Registration_Status__c": row["Registration Status"],
    }

    # Ensure a duplicate record is not created
    where_clause = ""
    for k, v in data.items():
        where_clause += f"{k}='{v}' AND "

    record_check = soql_to_df(
        f"SELECT Id, Name FROM Program_Term_Enrollment__c WHERE {where_clause[:-4]}",
        None,
    )
    if not record_check.empty:
        print("A record already exists, skipping")
        data_to_load.loc[index, "status"] = "Success"
    else:
        response = sf.Program_Term_Enrollment__c.create(data)
        if response["success"]:
            print(f"Sucess! Updated {row['LastName']}, {row['FirstName']}")
            data_to_load.loc[index, "status"] = "Success"
        else:
            print(f"Error Updating {row['LastName']}, {row['FirstName']}")
            data_to_load.loc[index, "status"] = response["errors"]

# Write out results and purge the input file
# selecting rows based on condition
write_results("program_term_enrollment_updates", data_to_load)