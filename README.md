
# Iowa-Liquor-Sales (HiveSQL & PySpark)

# Iowa-Liquor-Sales


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


### Methodology Cont:
"split_large_dataframe.r"
1.	Import the Data into RStudio: Begin by importing the original dataset Iowa_Liquor_Sales.csv into your R environment. This dataset contains around 28 million records.
2.	Set correct data types: Once Iowa_Liquor_Sales.csv has been loaded into RStudio use the str() function to adjust column data types appropriately.
3.	Split the data frame: Because Data Bricks only allows for 2GB file uploads (Iowa_Liquor_Sales.csv is around 7GB) Divide the dataset into four smaller data frames of approximately equal size. To achieve this, calculate the total number of records in the dataset and determine how many records each smaller data frame should contain. Then, assign each record to one of the four smaller data frames (roughly 7million records per data frame).
4.	Write the four split data frames to CSV Files: Once the dataset is divided into four smaller data frames, save each smaller data frame as a separate CSV file with header rows removed and the delimiter set to “|”.  This step allows us to upload each data frame (around 2GB each) individually to Data Bricks File Store (DBFS). The resulting CSV files are named Iowa_Liquor_Sales1.csv, Iowa_Liquor_Sales2.csv, Iowa_Liquor_Sales3, and Iowa_Liquor_Sales4.csv. These files can be located under the file store in our Data Bricks cluster at “/FileStore/dbfs/users/tnoga”. 

Data Bricks HiveSQL - "FinalProject.sql"

5.	Copy Files from File Store to own user in Data Bricks: Make sure to copy the original files to your own user in Data Bricks. This step ensures that we always have the original copy of data stored in FileStore. For example, copying Iowa_Liquor_Sales1.csv to your own user would look something like this: 
a.	%fs cp "/FileStore/dbfs/users/tnoga/Iowa_Liquor_Sales1.csv" "/users/tnoga/Iowa_Liquor_Sales1.csv"
6.	Table Creation: Create a new table in your own database using Hive SQL language to hold the liquor sales data. The table schema should include all the necessary columns from the CSV files with the appropriate data types. 
7.	Loading Data: Then dynamically load data from each CSV file into the partitioned table by using the LOAD DATA INPATH command and selecting the user’s CSV files. 
8.	Check records: Next create a simple query to ensure all records are loaded in the table. (28,792,399 records)
9.	Inspect Data: Create a simple query to ensure records look correct in the table.
10.	Check data types, columns, and missing values for all columns: Use the Analyze table and Compute statistics methods in HIVE SQL to check values for all columns in the table. We are specifically looking at missing values (Number of nulls for each column), min/max, and distinct counts to try and identify missing/incorrect data: (There were 21 records with missing data, so these records were removed as it won’t affect our analysis).
11.	Partitioning: Since some of our questions are about specific cities (Iowa City, Ames, etc.), create another table partitioned by the "City" column to improve query times. This means that data will be physically organized into separate directories on disk based on the values of the "City" column. Create additional partitioned tables as needed (years, alcohol types, etc.) *One important thing to note is that when creating partitioned tables, it takes a long time when dynamically loading the data into them. The increase in query speeds I saw from using the partitioned tables vs non-partitioned tables was in the ballpark of around twice as fast (4 seconds vs 8 seconds per query). However, since I am only doing a few queries, the time spent loading data into a new partitioned table is not worth the time saved when querying the data. If more queries were written, or this project was extended for a longer period, partitions would likely be more useful. *
12.	Queries: Once all the data has been loaded into Data Bricks, and sufficient EDA has been conducted to ensure data accuracy/redundancy, start creating queries to answer questions:
a.	Total number of bottles sold, sales amount, and gallons sold.
b.	Which cities sold the most alcohol by volume?
c.	Which cities sold the most alcohol by sales?
d.	Which year(s) sold the most alcohol?
e.	Which alcohol is most popular?
f.	Iowa City specific questions
i.	How much alcohol did we drink last year (2023)?
ii.	Which alcohol is most popular in Iowa City?
iii.	Where do people in Iowa City buy their liquor?
iv.	Which college city (Iowa city vs Ames) drank more alcohol (University of Iowa vs, Iowa State University)
g.	Where would be a good location for a liquor store in Iowa?

Data Bricks PySpark - FinalProject.Py

13.	PySpark: Next conduct time series analysis on annual liquor sales data using PySpark. First, import necessary libraries and load the sales data from HiveSQL table (tnoga.Iowa_Liquor_Sales) into a Pandas DataFrame. Next, extract and aggregate sales by year with sum(sales) as our target prediction. Once the data is loaded into an aggregated panda’s data frame by year, conduct the time series analysis using rolling statistics and the Dickey-Fuller test, which ensures the absence of trends or seasonality.  Next calculate autocorrelation and partial autocorrelation to determine the lag order for the SARIMAX model.
14.	Fit and train the SARIMAX model to forecast values for the years 2024 and 2025: Using PySpark conduct time series forecasting for liquor sales data using a SARIMA (Seasonal Autoregressive Integrated Moving Average) model. First import necessary libraries and load the sales data from the Hive table into a Pandas DataFrame. Make sure to extract and aggregate sales by year. Fit the SARIMA model to the sales data, specifying the order and seasonal order parameters determined from step 13.  After fitting the model, forecasts the sales for the years up to 2024 and extend the time series to include this forecasted value. Then retrain the SARIMA model using the extended time series data to incorporate the forecasted value of 2024. Finally, use this new model to forecast sales for the year 2025. 

### Results
Key findings include top-selling liquor categories, total sales volume and revenue, variations in sales across different cities and counties, seasonal trends, and predictive models for future sales. Insights specific to Iowa City highlight favorite alcohols, popular purchasing locations, and consumption statistics.
1.	What are the top-selling liquor categories in Iowa? (Product analysis): 
a.	Liquors:

i.	Fireball Cinamon Whiskey- 16,264,727 bottles sold.
ii.	Black Velvet – 14,068,512 bottles sold.
iii.	Hawkeye Vodka – 9,604,293 bottles sold.
iv.	Titos Vodka – 8,780,665 bottles sold.	
 
a.	Alcohol Categories:

i.	American Vodkas - 20.03% share of top 15 (44,075,550 bottles).
ii.	Canadian Whiskies - 15.26% share of top 15 (33,570,149 bottles).
iii.	Whiskey Liqueur – 12.27% share of top 15 (26,989,584 bottles).
iv.	Vodka 80 Proof – 8.24% share of top 15 (18,124,615 bottles).
v.	Spiced Rum – 8.04% share of top 15 (17,689,776 bottles).
 
2.	How much alcohol do Iowan’s buy/drink (2012-2024)?:
   
a.	Bottles sold – 311,006,875 bottles.
b.	 Sales amount ($USD) - $4,167,500,484.17.
c.	Gallons sold – 69,494,131 gallons.

4.	How do liquor sales vary across different counties and cities in Iowa? (Location analysis):
   
a.	Gallons Sold by City:

i.	Des Moines – 7,570,256 bottles.
ii.	Cedar Rapids – 4,55,982 bottles.
iii.	Davenport – 3,386,858 bottles. 
iv.	West Des Moines – 2,513,230 bottles.
v.	Council Bluffs – 2,263,666 bottles. 
 
b.	($USD) Sales by City:

i.	Des Moines – $507,143,084.72
ii.	Cedar Rapids – $274,515,440.67
iii.	Davenport – $208,386,044.02
iv.	West Des Moines – $167,438,190.36
v.	Waterloo – $141,333,936.34
 
4.	Are there any seasonal trends or patterns in liquor sales? (Date analysis):
   
a.	Continued upward growth.
i.	Growing market!	
b.	~Half billion USD$ every year in Iowa.
c.	2024 Jan-Mar Sales: $102,238,458 
 
6.	Can we predict future liquor sales based on historical data? (Forecasting model):
   
Rolling Mean & Std Deviation prediction:

a.	Using SARIMAX forecasting:
i.	Forecasted sales 2024: $616,192,700.
1.	Actual sales so far (3 months 2024 Jan-Mar): $102,238,458
2.	Sales increase in the summer months (June-Aug)
3.	Sales also increase in the month of December. 
a.	Estimate might be a bit on the high side but could be likely.
ii.	Forecasted sales 2025: $619,093,200.
b.	Using average growth 2012-2023:
i.	2024 sales = (1+ (((446m-255m)/255m) / 12 years))*446m = $474M
c.	Average between both forecasting models:
i.	2024 sales: (474M+619M)/2 = $546.5M
6.	Specific questions for the city “Iowa City”:
a.	Favorite alcohols? (Iowa City likes Vodka & Whiskey)
i.	Hawkeye Vodka - 364,234 bottles.
ii.	Titos Vodka – 328,805 bottles.
iii.	Fireball Whiskey – 267,834 bottles.
iv.	Barton Vodka – 267,277 bottles.
v.	Black Velvet Whiskey – 200,780 bottles.

=======
### Results
Key findings include top-selling liquor categories, total sales volume and revenue, variations in sales across different cities and counties, seasonal trends, and predictive models for future sales. Insights specific to Iowa City highlight favorite alcohols, popular purchasing locations, and consumption statistics.
>>>>>>> Stashed changes

### Conclusion/Implications
The analysis provides valuable insights for stakeholders in the Iowa liquor industry, including retailers, policymakers, and potential investors. Recommendations for potential liquor store locations based on sales and competition analysis offer actionable insights for entrepreneurs seeking opportunities in the market.

### Files
Iowa_Liquor_Sales.csv: Main dataset 28.8 Million Recoreds
split_large_dataframe.r: R script for data cleaning and splitting main file, resulting datasets are Iowa_Liquor_Sales1-4.csv
Iowa_Liquor_Sales1.csv, Iowa_Liquor_Sales2.csv, Iowa_Liquor_Sales3.csv, Iowa_Liquor_Sales4.csv: Partitioned dataset files that are 2GB each to load into DataBricks
FinalProject.sql: Databricks notebook containing HiveSQL and PySpark code
