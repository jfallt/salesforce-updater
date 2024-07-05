from simple_salesforce import Salesforce, format_soql
import pandas as pd
import yaml
from pathlib import Path
import os


# Config and Auth
def get_settings():
    full_file_path = Path(__file__).parent.joinpath("settings.yaml")
    with open(full_file_path) as settings:
        settings_data = yaml.load(settings, Loader=yaml.Loader)
    return settings_data


config = get_settings()

sf = Salesforce(
    version=config["API"]["version"],
    instance=config["API"]["base_url"] + config["API"]["version"],
    username=config["AUTH"]["username"],
    password=config["AUTH"]["password"],
    security_token=config["AUTH"]["security_token"],
)
path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../..", "Documents")
    )

# Funcs
def sql_where_convert(df_column):
    return ",".join([f"'{value}'" for value in df_column.to_list()])


def soql_to_df(query, column_aliases=None):
    print(query)
    df = pd.DataFrame(pd.DataFrame(sf.query(format_soql(query)))["records"].to_list())

    if "attributes" in df:
        df = df.drop(columns="attributes")
    if column_aliases:
        df.columns = column_aliases
    return df


def load_input_file(file_name):
    return pd.read_excel(os.path.join(path, f"{config['CONFIG'][file_name]}.xlsx"))


def get_contact_id(input_data: pd.DataFrame):
    """
    Add a column with the required salesforce id, matching on email
    """
    student_emails = sql_where_convert(input_data["Email"])
    students = soql_to_df(
        
        f"SELECT Id, Active_Program_Enrollment__c, email FROM Contact WHERE email IN ({student_emails})",
        ["Student__c", "Program_Enrollment__c", "email_match"],
    )
    return input_data.merge(
        students, left_on="Email", right_on="email_match", how="left"
    ).drop(columns="email_match")

def write_results(input_file: str, data_load: pd.DataFrame):
    df_success = data_load[data_load["status"] == "Success"]
    df_fail = data_load[data_load["status"] != "Success"]
    if not df_success.empty:
        df_success.to_excel(
            os.path.join(path, f"{config['CONFIG'][input_file]}_SUCCESS.xlsx"),
            index=False,
        )
    if not df_fail.empty:
        df_fail.to_excel(
            os.path.join(path, f"{config['CONFIG'][input_file]}_FAIL.xlsx"),
            index=False,
        )