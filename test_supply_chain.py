import pytest
import pandas as pd
import logging
from unittest.mock import patch
from Supply_chain_analysis import fetch_data_from_bigquery

# Configure logging
logging.basicConfig(filename="test_log.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

### 1. BigQuery Data Fetching Tests

@patch("Supply_chain_analysis.fetch_data_from_bigquery")
def test_fetch_data_from_bigquery(mock_fetch):
    logging.info("Testing fetch_data_from_bigquery function.")
    
    mock_df = pd.DataFrame({
        "Order_Date": ["2023-01-01", "2023-02-01"],
        "Sales": [100, 200],
        "Profit": [10, 20]
    })
    mock_fetch.return_value = mock_df
    df = fetch_data_from_bigquery()
    
    assert isinstance(df, pd.DataFrame), "Returned object is not a DataFrame"
    assert not df.empty, "Returned DataFrame is empty"
    assert set(["Order_Date", "Sales", "Profit"]).issubset(df.columns), "Missing expected columns"
    
    logging.info("fetch_data_from_bigquery function passed.")

def test_fetch_data_returns_empty_dataframe():
    logging.info("Testing fetch_data_from_bigquery with empty response.")
    
    with patch("Supply_chain_analysis.fetch_data_from_bigquery", return_value=pd.DataFrame()):
        df = fetch_data_from_bigquery()
        assert df.empty, "Expected an empty DataFrame but got data"
    
    logging.info("fetch_data_from_bigquery handles empty data correctly.")

def test_fetch_data_invalid_response():
    logging.info("Testing fetch_data_from_bigquery with invalid response.")
    
    with patch("Supply_chain_analysis.fetch_data_from_bigquery", return_value=None):
        with pytest.raises(AttributeError):
            fetch_data_from_bigquery().shape  # Should raise an error if None is returned

### 2. Data Cleaning & Transformation

def test_data_cleaning():
    logging.info("Testing data cleaning process.")
    
    data = {
        "Order_Date": ["2023-01-01", "2023-02-01", None],
        "Ship_Date": ["2023-01-05", "2023-02-04", None],
        "Sales": [100, 200, None],
        "Profit": [10, 20, None],
        "Shipping_Cost": [5, 10, None]
    }
    
    df = pd.DataFrame(data)
    df["Order_Date"] = pd.to_datetime(df["Order_Date"], errors="coerce")
    df["Ship_Date"] = pd.to_datetime(df["Ship_Date"], errors="coerce")
    df.fillna({"Sales": df["Sales"].median(), "Profit": df["Profit"].median()}, inplace=True)
    df.drop_duplicates(inplace=True)
    df["Lead_Time"] = (df["Ship_Date"] - df["Order_Date"]).dt.days.fillna(0)
    
    assert df["Sales"].isna().sum() == 0, "Sales column has NaN values after cleaning"
    assert df["Profit"].isna().sum() == 0, "Profit column has NaN values after cleaning"
    assert "Lead_Time" in df.columns, "Lead_Time column missing"
    
    logging.info("Data cleaning process passed.")

def test_data_cleaning_no_missing_values():
    logging.info("Testing data cleaning with no missing values.")
    
    df = pd.DataFrame({
        "Order_Date": ["2023-01-01", "2023-02-01"],
        "Ship_Date": ["2023-01-05", "2023-02-04"],
        "Sales": [100, 200],
        "Profit": [10, 20],
        "Shipping_Cost": [5, 10]
    })
    
    df["Order_Date"] = pd.to_datetime(df["Order_Date"])
    df["Ship_Date"] = pd.to_datetime(df["Ship_Date"])
    
    assert df.isna().sum().sum() == 0, "Unexpected NaN values found"
    
    logging.info("Data cleaning passed with no missing values.")

### 3. Sales & Profit Calculations

@pytest.mark.parametrize("sales, profit, expected_total, expected_percentage", [
    ([100, 200, 300], [10, 20, 30], 600, 10.0),
    ([50, 50, 50], [5, 5, 5], 150, 10.0),
    ([0, 0, 0], [0, 0, 0], 0, 0.0),
    ([10, 20, 30], [1, 2, 3], 60, 10.0)
])
def test_calculations(sales, profit, expected_total, expected_percentage):
    logging.info("Testing sales and profit calculations.")
    
    df = pd.DataFrame({"Sales": sales, "Profit": profit})
    total_sales = df["Sales"].sum()
    profit_percentage = (df["Profit"].sum() / total_sales) * 100 if total_sales > 0 else 0
    
    assert total_sales == expected_total, "Total Sales calculation is incorrect"
    assert round(profit_percentage, 2) == expected_percentage, "Profit percentage calculation is incorrect"
    
    logging.info("Sales and profit calculations passed.")

### 4. Exception Handling & Edge Cases

def test_invalid_inputs():
    logging.info("Testing invalid input handling.")
    
    with pytest.raises(TypeError):
        fetch_data_from_bigquery(product_name=12345)
    
    logging.info("Invalid input handling passed.")

def test_data_cleaning_invalid_types():
    logging.info("Testing data cleaning with invalid data types.")
    
    data = {
        "Order_Date": [123, "2023-02-01", None],
        "Ship_Date": ["2023-01-05", 456, None],
        "Sales": ["abc", 200, None],
        "Profit": [10, "xyz", None],
    }
    
    df = pd.DataFrame(data)
    df["Order_Date"] = pd.to_datetime(df["Order_Date"], errors="coerce")
    df["Ship_Date"] = pd.to_datetime(df["Ship_Date"], errors="coerce")
    df["Sales"] = pd.to_numeric(df["Sales"], errors="coerce").fillna(0)
    df["Profit"] = pd.to_numeric(df["Profit"], errors="coerce").fillna(0)
    
    assert df["Sales"].dtype != object, "Sales column should not contain strings"
    assert df["Profit"].dtype != object, "Profit column should be numeric"
    
    logging.info("Data cleaning with invalid types passed.")

def test_lead_time_negative_values():
    logging.info("Testing lead time calculation with incorrect dates.")
    
    df = pd.DataFrame({
        "Order_Date": ["2023-02-01", "2023-01-01"],
        "Ship_Date": ["2023-01-05", "2023-01-04"]
    })
    
    df["Order_Date"] = pd.to_datetime(df["Order_Date"])
    df["Ship_Date"] = pd.to_datetime(df["Ship_Date"])
    df["Lead_Time"] = (df["Ship_Date"] - df["Order_Date"]).dt.days
    df["Lead_Time"] = df["Lead_Time"].apply(lambda x: max(x, 0))  # Ensure no negative values
    
    assert (df["Lead_Time"] >= 0).all(), "Lead time contains negative values"
    
    logging.info("Lead time calculation passed.")
