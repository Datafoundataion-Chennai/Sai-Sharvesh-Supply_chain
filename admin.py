import streamlit as st
from google.cloud import bigquery
import pandas as pd
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"own Credentials"

   
# Set up BigQuery client
client = bigquery.Client()

# Function to execute DML queries
def execute_dml(query, params=None):
    job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
    query_job = client.query(query, job_config=job_config)
    query_job.result()
    st.success("Query executed successfully!")

# Function to fetch data
def fetch_data(query):
    query_job = client.query(query)
    df = query_job.to_dataframe()
    return df

# Function to check user role
def get_user_role(user_email):
    # Replace with actual IAM role fetching logic
    admin_users = ["Admin01@gmail.com"]  # Define admin users
    return "Admin" if user_email in admin_users else "General User"

# Streamlit UI
st.title("BigQuery Data Visualization & Management")

# Simulated user login (Replace with actual authentication)
user_email = "Admin01@gmail.com"  # Example user
user_role = get_user_role(user_email)
st.sidebar.write(f"Logged in as: {user_email} ({user_role})")

# Admin Panel for DML operations
if user_role == "Admin":
    st.sidebar.subheader("Admin Panel - Manage Data")
    operation = st.sidebar.selectbox("Select Operation", ["Insert", "Update", "Delete"])
    table_options = {
    "datatable": "macro-aurora-434314-h7.Supplychainanalysis.Cleaneddata",
    "usertable": "macro-aurora-434314-h7.Supplychainanalysis.UserTable"
    }

    selected_table = st.sidebar.selectbox("Select Table", list(table_options.keys()))
    table_name = table_options[selected_table]
    if operation == "Insert":
        columns = st.sidebar.text_input("Columns (comma-separated)")
        values = st.sidebar.text_input("Values (comma-separated)")
        if st.sidebar.button("Execute Insert"):
            query = f"""
                INSERT INTO `{table_name}` ({columns})
                VALUES ({values})
            """
            execute_dml(query)

    elif operation == "Update":
        set_clause = st.sidebar.text_input("SET clause (e.g., column='value')")
        condition = st.sidebar.text_input("WHERE condition")
        if st.sidebar.button("Execute Update"):
            query = f"""
                UPDATE `{table_name}`
                SET {set_clause}
                WHERE {condition}
            """
            execute_dml(query)

    elif operation == "Delete":
        condition = st.sidebar.text_input("WHERE condition")
        if st.sidebar.button("Execute Delete"):
            query = f"""
                DELETE FROM `{table_name}`
                WHERE {condition}
            """
            execute_dml(query)

# General User & Admin can view data
st.subheader("Data Visualization")
data_query = f"SELECT * FROM `{table_name}`"
data = fetch_data(data_query)
st.write(data)
