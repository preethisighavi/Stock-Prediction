## Stock ETL Prediction DAG

### Project Overview
This project involves an ETL (Extract, Transform, Load) pipeline for retrieving, processing, and predicting stock data using Apache Airflow, Alpha Vantage API, and Snowflake. The project aims to provide an automated solution to gather stock data for specific symbols (IBM, AAPL), load it into Snowflake for further analysis, and predict future stock prices, including opening, high, low, close prices, and volume, based on historical data. The predictions are also loaded into Snowflake for easy access.



### Problem Statement
The stock market is highly dynamic, and investors need accurate and timely data to make informed decisions. Manually retrieving, transforming, and analyzing stock data can be time-consuming and error-prone. This project aims to solve this problem by automating the process of data retrieval, transformation, and prediction of key stock metrics, allowing investors and analysts to make better decisions with up-to-date information.



### Summary of Procedures
Extract Stock Data: Fetch daily stock data from Alpha Vantage API for specified symbols (IBM, AAPL).

Process Stock Data: Extract relevant fields (date, open, high, low, close, volume) and filter the most recent 90 days of data.

Load Data into Snowflake: Load processed data into Snowflake using merge to handle conflicts.

Predict Stock Prices: Use ARIMA model to predict open, high, low, close, and volume for the next 7 days.

Load Predictions into Snowflake: Load predictions into Snowflake for further analysis.



### Technologies Used
Apache Airflow: Used for orchestrating ETL and prediction tasks.

Alpha Vantage API: Used for extracting historical stock data.

Snowflake: Data warehouse used to store raw and predicted stock data.

Python (statsmodels ARIMA): Used for predicting future stock prices.



### Snowflake Table Descriptions
Raw Data Table (stock_db.raw_data.stock_prices): Columns include open, high, low, close, volume, date, and symbol.

Predicted Data Table (stock_db.predicted_data.stock_prices): Columns include date, symbol, predicted close



### Error Handling
Retries: Tasks retry up to 3 times with a 5-minute delay between retries.

SQL Transactions: Managed with BEGIN, COMMIT, and ROLLBACK to ensure consistency during data loading.

Logging: Errors are logged during data extraction, processing, loading, and prediction to assist in debugging.



### Conclusion
This project successfully demonstrates the use of a daily ETL pipeline to gather, process, and predict stock data. It automates the entire workflow, ensuring that data is consistently up to date, and enables future stock price predictions using ARIMA, helping in financial analysis and decision-making.


