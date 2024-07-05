from merrimack import get_contact_id, load_input_file, write_results, sf

# Load Data
data_to_load = load_input_file("registration_updates")

# [BEGIN Query Ids and Apply to Data]
print("Querying required IDs")

# Students (Contact)
data_to_load = get_contact_id(data_to_load)

print("IDs applied to data")
# [END Query Ids and Apply to Data]


# Data Update
data_to_load["status"] = ""
for index, row in data_to_load.iterrows():
    print(f"Updating {row['LastName']}, {row['FirstName']}")

    # Payload for updated
    data = {
        "Registration_Status__c": row["Registration Status"],
    }

    response = sf.hed__Program_Enrollment__c.update(record_id=row["Program_Enrollment__c"], data=data)
    print(response)
    if response in [200, 201, 204]:
        print(f"Sucess! Updated {row['LastName']}, {row['FirstName']}")
        data_to_load.loc[index, "status"] = "Success"
    else:
        print(f"Error Updating {row['LastName']}, {row['FirstName']}")
        data_to_load.loc[index, "status"] = response

# Write out results and purge the input file
# selecting rows based on condition
write_results("registration_updates", data_to_load)
