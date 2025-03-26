import streamlit as st
import pandas as pd
import plotly.express as px
import logging

# Configure logging
logging.basicConfig(filename="liveapp.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Streamlit page config
st.set_page_config(page_title="Supply Chain Data Analysis", layout="wide")

# Inject custom CSS
try:
    with open("style.css", "r") as f:
        css = f.read()
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
    logging.info("CSS file loaded successfully.")
except Exception as e:
    logging.error(f"Error loading CSS file: {e}")

# Title
st.title("📊 Supply Chain Data Analysis")

# File upload
uploaded_file = st.file_uploader("Upload Excel or CSV File", type=["csv", "xlsx"])

if uploaded_file:
    try:
        # Read file (CSV or Excel)
        file_extension = uploaded_file.name.split(".")[-1]
        if file_extension == "csv":
            df = pd.read_csv(uploaded_file, low_memory=False)
        else:
            df = pd.read_excel(uploaded_file)

        logging.info(f"File '{uploaded_file.name}' uploaded and read successfully.")

        # Show dataset preview
        st.subheader("🔍 Cleaned Dataset Preview")
        st.write(df)

        # Identify datetime columns and convert
        for col in df.select_dtypes(include=["object"]).columns:
            try:
                df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors='coerce')
            except Exception:
                logging.warning(f"Skipping datetime conversion for column: {col}")

        # Convert all object columns to string (fix PyArrow error)
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].astype(str)

        # Show dataset summary
        st.subheader("📊 Dataset Summary")
        st.write(df.describe())

        # Column selection for analysis
        numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()
        categorical_cols = df.select_dtypes(include=["object"]).columns.tolist()

        # Numeric Data Analysis
        if numeric_cols:
            st.subheader("📈 Numeric Data Visualization")
            selected_num_col = st.selectbox("Select Numeric Column", numeric_cols)

            if selected_num_col:
                fig1 = px.histogram(df, x=selected_num_col, title=f"Distribution of {selected_num_col}", nbins=30)
                st.plotly_chart(fig1, use_container_width=True)
                logging.info(f"Histogram plotted for {selected_num_col}")

        # Categorical Data Analysis
        if categorical_cols:
            st.subheader("📊 Categorical Data Analysis")
            selected_cat_col = st.selectbox("Select Categorical Column", categorical_cols)

            if selected_cat_col:
                category_counts = df[selected_cat_col].value_counts().reset_index()
                category_counts.columns = [selected_cat_col, 'count']

                fig2 = px.bar(category_counts, x=selected_cat_col, y="count", title=f"Category Distribution: {selected_cat_col}")
                st.plotly_chart(fig2, use_container_width=True)
                logging.info(f"Bar chart plotted for {selected_cat_col}")

        # Final message
        st.success("✅ Data Analysis Complete!")
        logging.info("Data analysis completed successfully.")

    except Exception as e:
        st.error("❌ Error processing file. Please check the log file for details.")
        logging.error(f"Error processing file: {e}")
