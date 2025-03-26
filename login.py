import streamlit as st
import hashlib
from google.cloud import bigquery
import subprocess

# Configure BigQuery client
client = bigquery.Client()
TABLE_ID = "macro-aurora-434314-h7.Supplychainanalysis.UserTable"

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def check_credentials(email, password):
    if not email or not password:
        return False, None
    
    query = f"""
    SELECT User_Name, Password FROM `{TABLE_ID}`
    WHERE Mail_ID = @mail
    """
    query_job = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("mail", "STRING", email)
        ]
    ))
    result = query_job.result().to_dataframe()
    
    if result.empty:
        return False, None
    
    stored_hashed_password = result.loc[0, "Password"]
    return stored_hashed_password == hash_password(password), email

def register_user(username, email, password):
    if not username or not email or not password:
        return
    hashed_password = hash_password(password)
    query = f"""
    INSERT INTO `{TABLE_ID}` (User_Name, Mail_ID, Password)
    VALUES (@username, @email, @password)
    """
    client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("username", "STRING", username),
            bigquery.ScalarQueryParameter("email", "STRING", email),
            bigquery.ScalarQueryParameter("password", "STRING", hashed_password)
        ]
    ))

def main():
    st.markdown("""<style>""" + open("style.css").read() + """</style>""", unsafe_allow_html=True)
    
    st.title("Login Page")
    
    if "login_success" not in st.session_state:
        st.session_state.login_success = False
    
    email = st.text_input("Email", key="login_email")
    password = st.text_input("Password", type="password", key="login_password")
    
    if st.button("Login"):
        success, user_email = check_credentials(email, password)
        if success:
            st.session_state.login_success = True
            st.success(f"Welcome, {email}!")
            
            if user_email == "Admin01@gmail.com":
                subprocess.Popen(["streamlit", "run", "Supply_chain_analysis_admin.py"])
            else:
                subprocess.Popen(["streamlit", "run", "Supply_chain_analysis.py"])
        else:
            st.session_state.login_success = False
            st.error("Invalid email or password")
    
    if st.button("Sign Up"):
        st.session_state.show_signup = True
    
    if st.session_state.get("show_signup", False):
        st.subheader("Create a New Account")
        new_username = st.text_input("New Username", key="signup_username")
        email = st.text_input("Email", key="signup_email")
        new_password = st.text_input("New Password", type="password", key="signup_password")
        confirm_password = st.text_input("Confirm Password", type="password", key="signup_confirm_password")
        
        if st.button("Register"):
            if new_password == confirm_password:
                register_user(new_username, email, new_password)
                st.success("Account created successfully!")
                st.session_state.show_signup = False
            else:
                st.error("Passwords do not match")

if __name__ == "__main__":
    main()