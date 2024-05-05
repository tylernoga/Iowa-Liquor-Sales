# Iowa-Liquor-Sales (HiveSQL & PySpark)


### Executive Summary
This is my most recent project that I completed my senior year (May 2024) for the class BAIS:4220 - Adv. Database Mgmt & Big Data. By using big data analytics techniques learned thorughout the class such as Hive SQL and PySpark on the Azure Databricks platform, the objective of this analysis is to leverage publicly available data to gain insights about Iowa liquor sales trends, patterns, and geographical variations for the Iowa Liquor Sales dataset which can be acquired from here: https://data.iowa.gov/Sales-Distribution/Iowa-Liquor-Sales/m3tr-qhgy/about_data.

### Dataset
The dataset consists of 28.8M records with 24 features from the years 2012-2024. Each row in the dataset represents an individual product purchase in Iowa of Class “E” liquor. The features include information such as date of sale, store details, liquor category, vendor information, and sales metrics. 

### Motivation
The project aims to answer various questions such as top-selling liquor categories, total alcohol consumption in Iowa, variations in liquor sales across cities, seasonal trends, and predictions for future sales. Additionally, specific inquiries tailored to Iowa City provide insights into local consumption patterns and preferences.

### Methodology
The analysis involves data cleaning & preparation in RStudio: Filename "split_large_dataframe.r"
- Removing row headers from file
- Correcting wrong data types (str, numeric, date, etc.)
- Changing delimiter from "," to a "|"
- Handling missing data
- Splitting main file (6.5GB) into 4 smaller CSV files (2GB each) for uploading to Data Bricks

Then, data exploration, visualization, and querying using Hive SQL on Azure Data Bricks platform:
- Creating & loading data dynamically into a HiveSql table 
- Creating partitioned tables by city, year, etc. to improve certain query's speeds
- Querying data to answer specific questions (most popular alcohols, sales volumes, etc.)

Last, forecasting liquor sales using PySpark on the Azure Data Bricks platform. 

### Results
Key findings include top-selling liquor categories, total sales volume and revenue, variations in sales across different cities and counties, seasonal trends, and predictive models for future sales. Insights specific to Iowa City highlight favorite alcohols, popular purchasing locations, and consumption statistics.

### Conclusion/Implications
The analysis provides valuable insights for stakeholders in the Iowa liquor industry, including retailers, policymakers, and potential investors. Recommendations for potential liquor store locations based on sales and competition analysis offer actionable insights for entrepreneurs seeking opportunities in the market.

### Files
Iowa_Liquor_Sales.csv: Main dataset 28.8 Million Recoreds
split_large_dataframe.r: R script for data cleaning and splitting main file, resulting datasets are Iowa_Liquor_Sales1-4.csv
Iowa_Liquor_Sales1.csv, Iowa_Liquor_Sales2.csv, Iowa_Liquor_Sales3.csv, Iowa_Liquor_Sales4.csv: Partitioned dataset files that are 2GB each to load into DataBricks
FinalProject.sql: Databricks notebook containing HiveSQL and PySpark code
