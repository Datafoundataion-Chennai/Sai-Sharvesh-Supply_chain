import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import altair as alt
import logging
from google.cloud import bigquery
from google.oauth2 import service_account
import subprocess
import datetime

def load_css():
    with open("style.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

load_css()
# logging
logging.basicConfig(filename="app.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

#Google Cloud credentials
try:
    credentials = service_account.Credentials.from_service_account_file("./macro-aurora-434314-h7-159bdf313dd7.json")
    client = bigquery.Client(credentials=credentials)
except Exception as e:
    st.error("Failed to initialize BigQuery client.")
    logging.error(f"BigQuery client initialization failed: {e}")


st.title("📊 Supply Chain Data Analysis with BigQuery")

#Change User
if st.sidebar.button("🔄🤵 Change User"):
    st.sidebar.write("Launching Login page")
    subprocess.Popen(["streamlit", "run", "login.py"])

#filters
st.sidebar.header("🔍 Search & Filter")
search_text = st.sidebar.text_input("🔎 Search by Product Name:")
selected_category = st.sidebar.selectbox("📂 Select Category:", ["All","Furniture","Office Supplies","Technology"])
selected_segment = st.sidebar.selectbox("👥 Select Segment:", ["All","Consumer","Corporate","Home Office"])
start_date = datetime.date(2011, 1, 1)
end_date = datetime.date(2014, 12, 31)
date_range = st.sidebar.date_input("📅 Select Date Range:", [start_date, end_date], min_value=start_date, max_value=end_date)

# Sorting options
st.sidebar.header("🔄 Sorting Options")
sort_column = st.sidebar.selectbox("Sort by:", ["Sales", "Profit", "Order_Date"])
sort_order = st.sidebar.radio("Order:", ["Ascending", "Descending"])

# Advanced Analysis Button
if st.sidebar.button("⚙️ Advanced Analysis"):
    st.sidebar.write("Launching Advanced Analysis...")
    subprocess.Popen(["streamlit", "run", "advanced_analysis.py"])



#fetch data from BigQuery
def fetch_data_from_bigquery(product_name=None, category=None, segment=None, date_range=None):
    try:
        if product_name and not isinstance(product_name, str):
            raise ValueError("Product name must be a string.")
        
        if category and category != "All" and not isinstance(category, str):
            raise ValueError("Category must be a string.")
        
        if segment and segment != "All" and not isinstance(segment, str):
            raise ValueError("Segment must be a string.")

        if date_range and (not isinstance(date_range, (list, tuple)) or len(date_range) != 2):
            raise ValueError("Date range must be a list or tuple of two dates.")

        query = "SELECT * FROM macro-aurora-434314-h7.Supplychainanalysis.Cleaneddata WHERE 1=1"

        if product_name:
            query += f" AND LOWER(Product_Name) LIKE '%{product_name.lower()}%'"

        if category and category != "All":
            query += f" AND LOWER(Category) = '{category.lower()}'"

        if segment and segment != "All":
            query += f" AND LOWER(Segment) = '{segment.lower()}'"

        if date_range:
            query += f" AND Order_Date BETWEEN '{date_range[0]}' AND '{date_range[1]}'"
        
        query += f" ORDER BY {sort_column} {'ASC' if sort_order == 'Ascending' else 'DESC'}"
        
        df = client.query(query).to_dataframe()
        return df
    except ValueError as ve:
        logging.error(f"Invalid input error: {ve}")
    except Exception as e:
        st.error("Error fetching data from BigQuery.")
        logging.error(f"Data fetch error: {e}")
        return pd.DataFrame()


#display data
if st.button("Fetch Data from BigQuery"):
    df = fetch_data_from_bigquery(search_text, selected_category, selected_segment, date_range)
    
    if df.empty:
        st.warning("⚠ No data found for the given filters.")
    else:
        try:
            df["Order_Date"] = pd.to_datetime(df["Order_Date"], errors="coerce")
            df["Ship_Date"] = pd.to_datetime(df["Ship_Date"], errors="coerce")

            df.fillna({
                "Discount": df["Discount"].median(),
                "Profit": df["Profit"].median(),
                "Sales": df["Sales"].median(),
                "Shipping_Cost": df["Shipping_Cost"].median()
            }, inplace=True)

            df.drop_duplicates(inplace=True)
            df["Lead_Time"] = (df["Ship_Date"] - df["Order_Date"]).dt.days.fillna(0)
            df["Inventory_Turnover"] = df["Sales"] / (df["Shipping_Cost"] + 1)
            
            df["Category"] = df["Category"].str.lower()
            df["Sub_Category"] = df["Sub_Category"].str.lower()
            df["Segment"] = df["Segment"].str.lower()
            
            st.success("✅ Data Cleaning Complete!")
            st.subheader("🧹 Cleaned Data Preview")
            st.dataframe(df)

            
            total_sales = df["Sales"].sum()
            total_cities = df["City"].nunique()
            profit_percentage = (df["Profit"].sum() / df["Sales"].sum()) * 100 if df["Sales"].sum() > 0 else 0
            sales_rate = df["Sales"].sum() / df["Order_ID"].nunique() if df["Order_ID"].nunique() > 0 else 0

            # Display key metrics
            st.subheader("📊 Key Metrics")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("💰 Total Sales", f"${total_sales:,.2f}")
            col2.metric("🏙️ Total Cities", f"{total_cities}")
            col3.metric("📈 Profit Percentage", f"{profit_percentage:.2f}%")
            col4.metric("⚡ Sales Rate", f"${sales_rate:,.2f} per order")


            
            # 📈 MOnthly Order Trend
            query = """
                SELECT 
                    EXTRACT(YEAR FROM Order_Date) AS Year, 
                    FORMAT_DATE('%Y-%m', Order_Date) AS Month, 
                    COUNT(DISTINCT Order_ID) AS Order_Count
                FROM `macro-aurora-434314-h7.Supplychainanalysis.Cleaneddata`
                GROUP BY Year, Month
                ORDER BY Year, Month;
            """
            df1 = client.query(query).to_dataframe()
            st.subheader("📊 Monthly Order Trend Data")
            st.dataframe(df1)
            st.subheader("📈 Monthly Order Trend")
            fig = px.line(df1, x="Month", y="Order_Count", markers=True, title="Monthly Order Trend")
            st.plotly_chart(fig)
            st.write("This line chart represents the number of unique orders placed each month, helping to identify seasonal trends and peak sales periods.")


            # 📊 Monthly Sales Trend
            query = """
                SELECT 
                    FORMAT_DATE('%Y-%m', Order_Date) AS Order_Month, 
                    SUM(Sales) AS Total_Sales
                FROM `macro-aurora-434314-h7.Supplychainanalysis.Cleaneddata`
                GROUP BY Order_Month
                ORDER BY Order_Month;
            """
            df_sales = client.query(query).to_dataframe()
            st.subheader("📊 Monthly Sales Data")
            st.dataframe(df_sales)
            st.subheader("📈 Monthly Sales Trend")
            fig = px.line(df_sales, x="Order_Month", y="Total_Sales", markers=True, title="Monthly Sales Trend")
            st.plotly_chart(fig)
            st.write("This graph showcases total monthly sales, revealing revenue trends over time and indicating periods of high or low sales performance.")


            # ⏳ Lead Time Distribution
            query = """
                SELECT Lead_Time
                FROM `macro-aurora-434314-h7.Supplychainanalysis.Cleaneddata`
                WHERE Lead_Time IS NOT NULL;
            """
            df_lead_time = client.query(query).to_dataframe()
            st.subheader("⏳ Lead Time Data")
            st.dataframe(df_lead_time)
            st.subheader("⏳ Lead Time Distribution")
            fig = px.histogram(df_lead_time, x="Lead_Time", nbins=30, title="Lead Time Distribution", color_discrete_sequence=["#636EFA"])
            st.plotly_chart(fig)
            st.write("The histogram represents the distribution of lead times for orders, helping to assess delivery efficiency and potential delays.")


            # 💰 Sales vs. Profit Scatter Plot
            query = """
                SELECT Sales, Profit, Category, Product_Name
                FROM `macro-aurora-434314-h7.Supplychainanalysis.Cleaneddata`
                WHERE Sales IS NOT NULL AND Profit IS NOT NULL;
            """
            df_sales_profit = client.query(query).to_dataframe()
            st.subheader("💰 Sales vs. Profit Data")
            st.dataframe(df_sales_profit)
            st.subheader("💰 Sales vs. Profit Analysis")
            fig = px.scatter(df_sales_profit, x="Sales",y="Profit", color="Category", title="Sales vs. Profit", hover_data=["Product_Name"])
            st.plotly_chart(fig)
            st.write("This scatter plot visualizes the relationship between sales and profit across different product categories, helping to identify high-profit and low-profit products.")


            # 📊 Category-wise Sales Performance
            query = """
                SELECT Category, SUM(Sales) AS Total_Sales
                FROM `macro-aurora-434314-h7.Supplychainanalysis.Cleaneddata`
                WHERE Sales IS NOT NULL
                GROUP BY Category
                ORDER BY Total_Sales DESC;
            """
            df_category_sales = client.query(query).to_dataframe()
            st.subheader("📊 Category-wise Sales Data")
            st.dataframe(df_category_sales)
            st.subheader("📊 Category-wise Sales Performance")
            fig = px.pie(df_category_sales, names="Category", values="Total_Sales", title="Sales by Category", color_discrete_sequence=px.colors.qualitative.Pastel)
            st.plotly_chart(fig)
            st.write("This pie chart breaks down total sales by product category, allowing easy identification of the most and least revenue-generating categories.")


            # 📎 Inventory Turnover Distribution
            query = """
                SELECT Inventory_Turnover
                FROM `macro-aurora-434314-h7.Supplychainanalysis.Cleaneddata`
                WHERE Inventory_Turnover IS NOT NULL;
            """
            df_inventory_turnover = client.query(query).to_dataframe()
            st.subheader("📎 Inventory Turnover Data")
            st.dataframe(df_inventory_turnover)
            st.subheader("📎 Inventory Turnover Distribution")
            fig = px.histogram(df_inventory_turnover, x="Inventory_Turnover", nbins=30, title="Inventory Turnover Distribution", color_discrete_sequence=["#EF553B"])
            st.plotly_chart(fig)
            st.write("This histogram shows the distribution of inventory turnover rates, which helps evaluate how efficiently inventory is managed.")


            # 📊 Segment-wise Sales Performance
            query = """
                SELECT Segment, SUM(Sales) AS Total_Sales
                FROM `macro-aurora-434314-h7.Supplychainanalysis.Cleaneddata`
                WHERE Sales IS NOT NULL
                GROUP BY Segment
                ORDER BY Total_Sales DESC;
            """
            df_segment_sales = client.query(query).to_dataframe()
            st.subheader("📊 Segment-wise Sales Data")
            st.dataframe(df_segment_sales)
            st.subheader("📊 Segment-wise Sales Performance")
            fig = px.bar(df_segment_sales, x="Segment", y="Total_Sales", title="Sales by Segment", color="Segment", color_discrete_sequence=px.colors.qualitative.Set3)
            st.plotly_chart(fig)
            st.write("This bar chart displays total sales by customer segment, helping to understand which segments contribute the most revenue.")

            st.success("✅ Data Analysis & Visualization Complete!")
        except Exception as e:
            st.error("Error processing data for visualization.")
            logging.error(f"Visualization error: {e}")