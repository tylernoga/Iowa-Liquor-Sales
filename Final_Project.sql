-- Databricks notebook source
-- DBTITLE 1,Copy Code to Your Own User
-- MAGIC %fs cp "/FileStore/dbfs/users/tnoga/Iowa_Liquor_Sales1.csv" "/users/tnoga/Iowa_Liquor_Sales1.csv"

-- COMMAND ----------

-- MAGIC %fs cp "/FileStore/dbfs/users/tnoga/Iowa_Liquor_Sales2.csv" "/users/tnoga/Iowa_Liquor_Sales2.csv"

-- COMMAND ----------

-- MAGIC %fs cp "/FileStore/dbfs/users/tnoga/Iowa_Liquor_Sales3.csv" "/users/tnoga/Iowa_Liquor_Sales3.csv"

-- COMMAND ----------

-- MAGIC %fs cp "/FileStore/dbfs/users/tnoga/Iowa_Liquor_Sales4.csv" "/users/tnoga/Iowa_Liquor_Sales4.csv"

-- COMMAND ----------

-- DBTITLE 1,Create Original Table and Load all data

USE tnoga;

--drop table if exists Iowa_Liquor_Sales;

CREATE TABLE IF NOT EXISTS tnoga.Iowa_Liquor_Sales (
    `Invoice/Item` STRING,
    `Date_Col` DATE,
    `Store_Number` STRING,
    `Store_Name` STRING,
    `Address` STRING,
    `City` STRING,
    `Zip_Code` STRING,
    `Store_Location` STRING,
    `County_Number` STRING,
    `County` STRING,
    `Category` STRING,
    `Category_Name` STRING,
    `Vendor_Number` STRING,
    `Vendor_Name` STRING,
    `Item_Number` STRING,
    `Item_Description` STRING,
    `Pack` INT,
    `Bottle_Volume_(ml)` DOUBLE,
    `State_Bottle_Cost` DOUBLE,
    `State_Bottle_Retail` DOUBLE,
    `Bottles_Sold` INT,
    `Sale_(Dollars)` DOUBLE,
    `Volume_Sold_(Liters)` DOUBLE,
    `Volume_Sold_(Gallons)` DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;



-- Load data from each CSV file into the combined table
LOAD DATA INPATH '/users/tnoga/Iowa_Liquor_Sales1.csv' INTO TABLE tnoga.Iowa_Liquor_Sales;
LOAD DATA INPATH '/users/tnoga/Iowa_Liquor_Sales2.csv' INTO TABLE tnoga.Iowa_Liquor_Sales;
LOAD DATA INPATH '/users/tnoga/Iowa_Liquor_Sales3.csv' INTO TABLE tnoga.Iowa_Liquor_Sales;
LOAD DATA INPATH '/users/tnoga/Iowa_Liquor_Sales4.csv' INTO TABLE tnoga.Iowa_Liquor_Sales;

-- COMMAND ----------

-- DBTITLE 1,Make sure all the data loaded in (28,792,399)
--Number of data points in table
use tnoga;

select count(*) as Rows_Of_Data
FROM Iowa_Liquor_Sales;

-- COMMAND ----------

-- DBTITLE 1,Check how many distinct cities
--Check the number of cities in table
SELECT COUNT(DISTINCT City)
FROM tnoga.Iowa_Liquor_Sales;

-- COMMAND ----------

-- DBTITLE 1,Create a new table partitioned by city for efficient queries
-- Create a partitioned table by city to enhance city-specific queries


SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 3000;



--DROP TABLE IF EXISTS tnoga.Iowa_Liquor_Sales_cities;

-- Create the table
CREATE TABLE IF NOT EXISTS tnoga.Iowa_Liquor_Sales_cities (
    `Invoice/Item` STRING,
    `Date_Col` DATE,
    `Store_Number` STRING,
    `Store_Name` STRING,
    `Address` STRING,
    `City` STRING,
    `Zip_Code` STRING,
    `Store_Location` STRING,
    `County_Number` STRING,
    `County` STRING,
    `Category` STRING,
    `Category_Name` STRING,
    `Vendor_Number` STRING,
    `Vendor_Name` STRING,
    `Item_Number` STRING,
    `Item_Description` STRING,
    `Pack` STRING,
    `Bottle_Volume_(ml)` DOUBLE,
    `State_Bottle_Cost` DOUBLE,
    `State_Bottle_Retail` DOUBLE,
    `Bottles_Sold` INT,
    `Sale_(Dollars)` DOUBLE,
    `Volume_Sold_(Liters)` DOUBLE,
    `Volume_Sold_(Gallons)` DOUBLE
)
PARTITIONED BY (Distinct_City STRING)
STORED AS TEXTFILE;

-- Load in data to table 
INSERT INTO TABLE tnoga.Iowa_Liquor_Sales_cities PARTITION (Distinct_City)
SELECT 
    `Invoice/Item`,
    `Date_Col`,
    `Store_Number`,
    `Store_Name`,
    `Address`,
    `City`,
    `Zip_Code`,
    `Store_Location`,
    `County_Number`,
    `County`,
    `Category`,
    `Category_Name`,
    `Vendor_Number`,
    `Vendor_Name`,
    `Item_Number`,
    `Item_Description`,
    `Pack`,
    `Bottle_Volume_(ml)`,
    `State_Bottle_Cost`,
    `State_Bottle_Retail`,
    `Bottles_Sold`,
    `Sale_(Dollars)`,
    `Volume_Sold_(Liters)`,
    `Volume_Sold_(Gallons)`,
    City as Distinct_City
FROM Iowa_Liquor_Sales;




-- COMMAND ----------

-- DBTITLE 1,Make sure we have 492 partitions
use tnoga;
b
SHOW PARTITIONS tnoga.Iowa_Liquor_Sales_cities;

-- COMMAND ----------

--Check the number of years in table
SELECT COUNT(DISTINCT year(Date_Col))
FROM tnoga.Iowa_Liquor_Sales;

-- COMMAND ----------

-- DBTITLE 1,Create another table partitioned by year
-- Create a partitioned table by year to enhance year-specific queries

--DROP TABLE IF EXISTS tnoga.Iowa_Liquor_Sales_years;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 5000;



CREATE TABLE IF NOT EXISTS tnoga.Iowa_Liquor_Sales_years (
    `Invoice/Item` STRING,
    `Date_Col` DATE,
    `Store_Number` STRING,
    `Store_Name` STRING,
    `Address` STRING,
    `City` STRING,
    `Zip_Code` STRING,
    `Store_Location` STRING,
    `County_Number` STRING,
    `County` STRING,
    `Category` STRING,
    `Category_Name` STRING,
    `Vendor_Number` STRING,
    `Vendor_Name` STRING,
    `Item_Number` STRING,
    `Item_Description` STRING,
    `Pack` STRING,
    `Bottle_Volume_(ml)` DOUBLE,
    `State_Bottle_Cost` DOUBLE,
    `State_Bottle_Retail` DOUBLE,
    `Bottles_Sold` INT,
    `Sale_(Dollars)` DOUBLE,
    `Volume_Sold_(Liters)` DOUBLE,
    `Volume_Sold_(Gallons)` DOUBLE
)
PARTITIONED BY (Year_Date INT)
STORED AS TEXTFILE;




-- Load in data to table 
INSERT OVERWRITE TABLE tnoga.Iowa_Liquor_Sales_years PARTITION (Year_Date)
SELECT 
    *,
    YEAR(Date_Col) AS Year_Date
FROM tnoga.Iowa_Liquor_Sales;


-- COMMAND ----------

-- DBTITLE 1,Data inspection
--inspect data for any errors

use tnoga;
SELECT *
FROM Iowa_Liquor_Sales;

-- COMMAND ----------

-- DBTITLE 1,EDA, Collumn, Data types
--Check data types, columns, and missing values for all columns

use tnoga;

ANALYZE TABLE Iowa_Liquor_Sales COMPUTE STATISTICS FOR ALL COLUMNS;

DESCRIBE FORMATTED Iowa_Liquor_Sales `Invoice/Item`;
DESCRIBE FORMATTED Iowa_Liquor_Sales Date_Col;
DESCRIBE FORMATTED Iowa_Liquor_Sales Store_Number;
DESCRIBE FORMATTED Iowa_Liquor_Sales Store_Name;
DESCRIBE FORMATTED Iowa_Liquor_Sales Address;
DESCRIBE FORMATTED Iowa_Liquor_Sales City;
DESCRIBE FORMATTED Iowa_Liquor_Sales Zip_Code;
DESCRIBE FORMATTED Iowa_Liquor_Sales Store_Location;
DESCRIBE FORMATTED Iowa_Liquor_Sales County_Number;
DESCRIBE FORMATTED Iowa_Liquor_Sales Category;
DESCRIBE FORMATTED Iowa_Liquor_Sales Category_Name;
DESCRIBE FORMATTED Iowa_Liquor_Sales Vendor_Number;
DESCRIBE FORMATTED Iowa_Liquor_Sales Vendor_Name;
DESCRIBE FORMATTED Iowa_Liquor_Sales Item_Number;
DESCRIBE FORMATTED Iowa_Liquor_Sales Item_Description;
DESCRIBE FORMATTED Iowa_Liquor_Sales Pack;
DESCRIBE FORMATTED Iowa_Liquor_Sales `Bottle_Volume_(ml)`;
DESCRIBE FORMATTED Iowa_Liquor_Sales State_Bottle_Cost;
DESCRIBE FORMATTED Iowa_Liquor_Sales State_Bottle_Retail;
DESCRIBE FORMATTED Iowa_Liquor_Sales Bottles_Sold;
DESCRIBE FORMATTED Iowa_Liquor_Sales `Sale_(Dollars)`;
DESCRIBE FORMATTED Iowa_Liquor_Sales `Volume_Sold_(Liters)`;
DESCRIBE FORMATTED Iowa_Liquor_Sales `Volume_Sold_(Gallons)`;

-- COMMAND ----------

--Query for entire database to get basic idea and answer questions
--Total number of bottles sold, sales amount, and gallons sold
SELECT 
    SUM(Bottles_Sold) AS Total_Bottles_Sold,
    SUM(`Sale_(Dollars)`) AS Total_Sales_Amount,
    SUM(`Volume_Sold_(Gallons)`) AS Total_Gallons_Sold
FROM Iowa_Liquor_Sales;

-- COMMAND ----------

--Which cities sold the most alcohol by volume (Gallons)?
SELECT 
    City,
    SUM(`Volume_Sold_(Gallons)`) AS Total_Gallons_Sold
FROM Iowa_Liquor_Sales
GROUP BY City
ORDER BY Total_Gallons_Sold DESC
LIMIT 10;

-- COMMAND ----------

--Which cities sold the most alcohol in Iowa by sales (Dollars)?
SELECT 
    City,
    SUM(`Sale_(Dollars)`) AS Total_Gallons_Sold
FROM Iowa_Liquor_Sales
GROUP BY City
ORDER BY Total_Gallons_Sold DESC
LIMIT 10;

-- COMMAND ----------

--Year analysis
--Which years sold the most alcohol?
SELECT 
    YEAR(Date_Col) AS Sales_Year,
    SUM(`Sale_(Dollars)`) AS Total_Sales_Amount
FROM Iowa_Liquor_Sales
GROUP BY Sales_Year
ORDER BY Sales_Year;

-- COMMAND ----------

--Which alcohol is most popular in Iowa?
SELECT 
    Item_Description,
    SUM(Bottles_Sold) AS Total_Bottles_Sold
FROM Iowa_Liquor_Sales
GROUP BY Item_Description
ORDER BY Total_Bottles_Sold DESC
LIMIT;

-- COMMAND ----------

--What type of alcohol does Iowa like?
SELECT 
    Category_Name,
    SUM(Bottles_Sold) AS Total_Bottles_Sold
FROM Iowa_Liquor_Sales
GROUP BY Category_Name
ORDER BY Total_Bottles_Sold DESC
Limit 15;


-- COMMAND ----------

--Which alcohol was most popular in Iowa City

SELECT 
    Item_Description,
    SUM(Bottles_Sold) AS Total_Bottles_Sold
FROM tnoga.iowa_liquor_sales
WHERE City LIKE '%IOWA CITY%'
GROUP BY Item_Description
ORDER BY Total_Bottles_Sold DESC
LIMIT 5;


-- COMMAND ----------

--How much alcohol did Iowa City drink in 2023? (9 seconds)
SELECT 
    SUM(`Volume_Sold_(Gallons)`) AS Total_Gallons_Sold,
    SUM(`Sale_(Dollars)`) AS Total_Sales_Amount,
    SUM(Bottles_Sold) AS Total_Bottles_Sold

FROM Iowa_Liquor_Sales
WHERE City LIKE '%IOWA CITY%'
AND YEAR(Date_Col) = 2023;

-- COMMAND ----------

-- Check query speed difference on partitioned vs non partitioned table (3 seconds)
    SUM(`Volume_Sold_(Gallons)`) AS Total_Gallons_Sold,
    SUM(`Sale_(Dollars)`) AS Total_Sales_Amount,
    SUM(Bottles_Sold) AS Total_Bottles_Sold
FROM tnoga.Iowa_Liquor_Sales_years
WHERE City LIKE '%IOWA CITY%'  
  AND Year_Date = 2023;         


-- COMMAND ----------

--Where do people in Iowa City buy alcohol?
SELECT 
Store_Name, SUM(`Volume_Sold_(Gallons)`) AS Total_Gallons_Sold
FROM Iowa_Liquor_Sales
WHERE City LIKE '%IOWA CITY%'
GROUP BY Store_Name
ORDER BY Total_Gallons_Sold DESC
LIMIT 10;




-- COMMAND ----------

--Which college city drank more alcohol (Iowa City vs Ames)
SELECT 
    City,
    SUM(`Volume_Sold_(Gallons)`) AS Total_Gallons_Sold
FROM Iowa_Liquor_Sales
WHERE City LIKE '%IOWA CITY%' OR City LIKE '%AMES%'
GROUP BY City
ORDER BY Total_Gallons_Sold DESC;


-- COMMAND ----------

--A query where I take the count of liquor stores in each city divided by the sum of sales of that city
-- A lower Liquor_Store_to_Sales_ratio indicates a larger demand for alcohol with not as many stores to fill it
--This would mean more sales for each liquor store and might indicate a good location to open a liquor store 
SELECT 
    City,
    COUNT(DISTINCT Store_Number) AS Num_Liquor_Stores,
    SUM(`Sale_(Dollars)`) AS Total_Sales_Amount,
    COUNT(DISTINCT Store_Number) / SUM(`Sale_(Dollars)`) AS Liquor_Store_to_Sales_Ratio,
    SUM(`Sale_(Dollars)`) / COUNT(DISTINCT Store_Number)  AS Sales_Per_Liquor_Store
FROM 
    tnoga.Iowa_Liquor_Sales
WHERE
    City IS NOT NULL AND City  NOT LIKE '%POLK%'
GROUP BY 
    City
ORDER BY 
    Liquor_Store_to_Sales_Ratio ASC;


-- COMMAND ----------

-- DBTITLE 1,Rolling Average/Rolling STD Predictions on Sales
-- MAGIC %python
-- MAGIC # Import necessary libraries
-- MAGIC import pandas as pd
-- MAGIC import numpy as np
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC from statsmodels.tsa.statespace.sarimax import SARIMAX
-- MAGIC from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
-- MAGIC from statsmodels.tsa.stattools import adfuller
-- MAGIC from sklearn.metrics import mean_squared_error
-- MAGIC
-- MAGIC # Load data from Hive table into DataFrame
-- MAGIC liquor_sales_df = spark.table("tnoga.Iowa_Liquor_Sales").toPandas()
-- MAGIC
-- MAGIC # Extract year from Date_Col column
-- MAGIC liquor_sales_df['Year'] = pd.to_datetime(liquor_sales_df['Date_Col']).dt.year
-- MAGIC
-- MAGIC # Aggregate sales by year
-- MAGIC sales_by_year_df = liquor_sales_df.groupby('Year')['Sale_(Dollars)'].sum()
-- MAGIC
-- MAGIC # Convert to time series
-- MAGIC sales_ts = sales_by_year_df
-- MAGIC
-- MAGIC # Filter out data for the year 2024 as there is only 3 months of data
-- MAGIC sales_data = sales_ts[sales_ts.index != 2024]
-- MAGIC
-- MAGIC # Plot the time series
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC plt.plot(sales_data.index, sales_data.values, marker='o')
-- MAGIC plt.title('Annual Liquor Sales')
-- MAGIC plt.xlabel('Year')
-- MAGIC plt.ylabel('Sales (Dollars)')
-- MAGIC plt.grid(True)
-- MAGIC plt.show()
-- MAGIC
-- MAGIC # Check for stationarity
-- MAGIC def check_stationarity(timeseries):
-- MAGIC     # Calculate rolling statistics
-- MAGIC     rolling_mean = timeseries.rolling(window=5).mean()
-- MAGIC     rolling_std = timeseries.rolling(window=5).std()
-- MAGIC     
-- MAGIC     # Plot rolling statistics
-- MAGIC     plt.figure(figsize=(10, 6))
-- MAGIC     plt.plot(timeseries, color='blue', label='Original')
-- MAGIC     plt.plot(rolling_mean, color='red', label='Rolling Mean')
-- MAGIC     plt.plot(rolling_std, color='black', label='Rolling Std')
-- MAGIC     plt.legend()
-- MAGIC     plt.title('Rolling Mean & Standard Deviation')
-- MAGIC     plt.show()
-- MAGIC
-- MAGIC     # Perform Dickey-Fuller test
-- MAGIC     print('Results of Dickey-Fuller Test:')
-- MAGIC     adf_test = adfuller(timeseries, autolag='AIC')
-- MAGIC     adf_output = pd.Series(adf_test[0:4], index=['Test Statistic', 'p-value', '#Lags Used', 'Number of Observations Used'])
-- MAGIC     for key, value in adf_test[4].items():
-- MAGIC         adf_output['Critical Value (%s)' % key] = value
-- MAGIC     print(adf_output)
-- MAGIC
-- MAGIC check_stationarity(sales_data)
-- MAGIC
-- MAGIC # Differencing to achieve stationarity
-- MAGIC sales_ts_diff = sales_data.diff().dropna()
-- MAGIC check_stationarity(sales_ts_diff)
-- MAGIC
-- MAGIC # Plot ACF and PACF
-- MAGIC print("Plotting ACF and PACF...")
-- MAGIC plt.figure(figsize=(14, 7))
-- MAGIC plt.subplot(211)
-- MAGIC plot_acf(sales_ts_diff, ax=plt.gca())
-- MAGIC plt.subplot(212)
-- MAGIC max_lags = min(len(sales_ts_diff) // 2 - 1, 5)  # Limiting to 5 lags or 50% of the sample size - 1
-- MAGIC plot_pacf(sales_ts_diff, ax=plt.gca(), lags=max_lags)
-- MAGIC plt.show()
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Forecasting for 2024-2025
-- MAGIC %python
-- MAGIC # Import necessary libraries
-- MAGIC import pandas as pd
-- MAGIC from statsmodels.tsa.statespace.sarimax import SARIMAX
-- MAGIC
-- MAGIC # Load data from Hive table into DataFrame
-- MAGIC liquor_sales_df = spark.table("tnoga.Iowa_Liquor_Sales").toPandas()
-- MAGIC
-- MAGIC # Extract year from Date_Col column
-- MAGIC liquor_sales_df['Year'] = pd.to_datetime(liquor_sales_df['Date_Col']).dt.year
-- MAGIC
-- MAGIC # Aggregate sales by year
-- MAGIC sales_by_year_df = liquor_sales_df.groupby('Year')['Sale_(Dollars)'].sum()
-- MAGIC
-- MAGIC # Filter out data for the year 2024
-- MAGIC sales_data = sales_by_year_df[sales_by_year_df.index != 2024]
-- MAGIC
-- MAGIC # Fit SARIMA model
-- MAGIC order = (1, 1, 1)
-- MAGIC seasonal_order = (1, 1, 0, 12)
-- MAGIC model = SARIMAX(sales_data, order=order, seasonal_order=seasonal_order, enforce_stationarity=False, enforce_invertibility=False)
-- MAGIC sarima_model = model.fit(disp=False)
-- MAGIC
-- MAGIC # Forecast for 2024
-- MAGIC forecast_2024 = sarima_model.forecast(steps=1)
-- MAGIC
-- MAGIC # Extend time series to include 2024
-- MAGIC sales_data_extended = sales_data.append(pd.Series(forecast_2024, index=[2024]))
-- MAGIC
-- MAGIC # Retrain SARIMA model with extended time series
-- MAGIC sarima_model_2024 = SARIMAX(sales_data_extended, order=order, seasonal_order=seasonal_order, enforce_stationarity=False, enforce_invertibility=False)
-- MAGIC sarima_model_2024 = sarima_model_2024.fit(disp=False)
-- MAGIC
-- MAGIC # Forecast for 2025
-- MAGIC forecast_2025 = sarima_model_2024.forecast(steps=1)
-- MAGIC
-- MAGIC print("Forecast for 2024:", forecast_2024)
-- MAGIC print("Forecast for 2025:", forecast_2025)
-- MAGIC
