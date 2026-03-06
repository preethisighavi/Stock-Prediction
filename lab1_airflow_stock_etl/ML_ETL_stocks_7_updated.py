#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
import logging
import time


default_args = {
    'owner': 'himajasree',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),  
}

with DAG(
    'stock_etl_prediction_dag_5',
    default_args=default_args,
    description='ETL and Prediction DAG for Stock Data using Alpha Vantage and Snowflake',
    schedule_interval='@daily',  
    start_date=datetime(2024, 10, 10),
    catchup=False,
    max_active_runs=2,  # Allow parallel execution for symbols
) as dag:

    symbols = ["IBM", "AAPL"]
    vantage_api_key = Variable.get('vantage_api_key', default_var='default_value_here')
    
    if vantage_api_key == 'default_value_here':
        logging.warning("Variable 'vantage_api_key' is not found, using default value.")

    def return_snowflake_conn():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        conn.cursor().execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 1800;")  # Increase timeout to 30 minutes
        return conn.cursor()

    @task
    def extract(symbol):
        try:
            url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            if "Time Series (Daily)" not in data:
                logging.error(f"Failed to fetch data for symbol: {symbol}. Response: {data}")
                return {}
            logging.info(f"Successfully fetched data for symbol: {symbol}")
            time.sleep(12)  
            return data
        except Exception as e:
            logging.error(f"Error occurred during data extraction for symbol: {symbol}. Error: {e}")
            return {}

    @task
    def process_stock_data(data, symbol):
        if not data:
            logging.warning(f"No data available for symbol: {symbol}. Skipping processing.")
            return []

        results = []
        for date, stock_info in data.get("Time Series (Daily)", {}).items():
            if date and isinstance(stock_info, dict):  
                stock_info["date"] = date
                stock_info["symbol"] = symbol
                results.append(stock_info)
        if not results:
            logging.warning(f"No valid data to process for symbol: {symbol}.")
        else:
            logging.info(f"Processed {len(results)} records for symbol: {symbol}")
        return results[:90]

    @task
    def load_data_to_snowflake(results, symbol):
        if not results:
            logging.warning(f"No data to load into Snowflake for symbol: {symbol}.")
            return
        
        cursor = return_snowflake_conn()
        table = "stock_db.raw_data.stock_prices"

        try:
            cursor.execute("BEGIN;")
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                "open" FLOAT,
                "high" FLOAT,
                "low" FLOAT,
                "close" FLOAT,
                "volume" INT,
                "date" DATE,
                "symbol" VARCHAR(20),
                PRIMARY KEY ("date", "symbol")
            );
            """
            cursor.execute(create_table_query)

            # insert data using MERGE 
            for r in results:
                merge_sql = f"""
                MERGE INTO {table} AS target
                USING (SELECT '{r['date']}' AS "date", '{r['symbol']}' AS "symbol", {float(r.get('1. open', 0))} AS "open", {float(r.get('2. high', 0))} AS "high", {float(r.get('3. low', 0))} AS "low", {float(r.get('4. close', 0))} AS "close", {int(r.get('5. volume', 0))} AS "volume") AS source
                ON target."date" = source."date" AND target."symbol" = source."symbol"
                WHEN MATCHED THEN
                    UPDATE SET "open" = source."open", "high" = source."high", "low" = source."low", "close" = source."close", "volume" = source."volume"
                WHEN NOT MATCHED THEN
                    INSERT ("date", "symbol", "open", "high", "low", "close", "volume")
                    VALUES (source."date", source."symbol", source."open", source."high", source."low", source."close", source."volume");
                """
                cursor.execute(merge_sql)

            cursor.execute("COMMIT;")
            logging.info(f"Successfully loaded data for symbol: {symbol} into Snowflake.")
        except Exception as e:
            cursor.execute("ROLLBACK;")
            logging.error("Error occurred while loading data to Snowflake for symbol: %s, Error: %s", symbol, e)
            raise e

    @task
    def predict_stock_prices(results, symbol):
        if not results:
            logging.warning(f"No data available for symbol: {symbol}. Skipping prediction.")
            return []

        
        df = pd.DataFrame(results)

        if 'date' not in df.columns:
            logging.error(f"Column 'date' not found in data for symbol: {symbol}. Skipping prediction.")
            return []

        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])
        df = df.sort_values('date')
        df.set_index('date', inplace=True)

        # Using closing prices for prediction
        if '4. close' not in df.columns:
            logging.error(f"Column '4. close' not found in data for symbol: {symbol}. Skipping prediction.")
            return []

        df['close'] = df['4. close'].astype(float)
        close_prices = df['close']

        try:
            
            recent_prices = close_prices.tail(60)  # Use only the most recent 60 days for prediction
            model = ARIMA(recent_prices, order=(5, 1, 0))
            model_fit = model.fit()

            # Predicting next 7 days
            forecast = model_fit.forecast(steps=7)
            forecast_dates = pd.date_range(start=df.index[-1] + pd.Timedelta(days=1), periods=7)

            
            predicted_data = []
            for date, price in zip(forecast_dates, forecast):
                predicted_data.append({
                    "date": date.strftime('%Y-%m-%d'),
                    "symbol": symbol,
                    "predicted_close": price
                })
            logging.info(f"Successfully predicted stock prices for symbol: {symbol}")
            return predicted_data
        except Exception as e:
            logging.error(f"Error occurred during prediction for symbol: {symbol}. Error: {e}")
            return []

    @task
    def load_predictions_to_snowflake(predicted_data, symbol):
        if not predicted_data:
            logging.warning(f"No predictions to load into Snowflake for symbol: {symbol}.")
            return
        
        cursor = return_snowflake_conn()
        table = "stock_db.predicted_data.stock_prices"

        try:
            cursor.execute("BEGIN;")
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                "date" DATE,
                "symbol" VARCHAR(20),
                "predicted_close" FLOAT,
                PRIMARY KEY ("date", "symbol")
            );
            """
            cursor.execute(create_table_query)

            # inserting predictions using MERGE 
            for r in predicted_data:
                merge_sql = f"""
                MERGE INTO {table} AS target
                USING (SELECT '{r['date']}' AS "date", '{r['symbol']}' AS "symbol", {r['predicted_close']} AS "predicted_close") AS source
                ON target."date" = source."date" AND target."symbol" = source."symbol"
                WHEN MATCHED THEN
                    UPDATE SET "predicted_close" = source."predicted_close"
                WHEN NOT MATCHED THEN
                    INSERT ("date", "symbol", "predicted_close")
                    VALUES (source."date", source."symbol", source."predicted_close");
                """
                cursor.execute(merge_sql)

            cursor.execute("COMMIT;")
            logging.info(f"Successfully loaded predictions for symbol: {symbol} into Snowflake.")
        except Exception as e:
            cursor.execute("ROLLBACK;")
            logging.error("Error occurred while loading predictions to Snowflake for symbol: %s, Error: %s", symbol, e)
            raise e

    for symbol in symbols:
        stock_data = extract(symbol)
        processed_data = process_stock_data(stock_data, symbol)
        load_data_to_snowflake(processed_data, symbol)
        predicted_data = predict_stock_prices(processed_data, symbol)
        load_predictions_to_snowflake(predicted_data, symbol)

