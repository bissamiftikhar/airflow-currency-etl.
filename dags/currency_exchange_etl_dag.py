"""
DAG: Currency Exchange ETL Pipeline
Integrates daily currency exchange rates from Frankfurter API to MongoDB after transformations.

Requirements satisfied:
1. API: Frankfurter API (base=EUR, quotes=USD,GBP,JPY)
2. Database: MongoDB with Mongo Express as web admin interface
3. Docker networking and persistent storage
4. Three distinct tasks:
   - Extract Currency Exchange Rates
   - Transform Exchange Rate Data
   - Load Data into MongoDB
5. Named XComs for task data communication
"""

from datetime import datetime, timezone, timedelta
import logging
import os
import requests
from pymongo import MongoClient

from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

API_URL = "https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:password123@mongodb:27017/")
DB_NAME = "currency_exchange_db"
COLLECTION_NAME = "daily_rates"


def extract_currency_exchange_rates(ti, **kwargs):
    """
    Task 1: Extract Currency Exchange Rates
    Calls the Frankfurter API and pushes raw data via Named XCom.
    """
    logging.info(f"Extracting exchange rate data from {API_URL}")
    response = requests.get(API_URL, timeout=15)
    response.raise_for_status()
    raw_data = response.json()

    logging.info(f"Successfully retrieved raw data: {raw_data}")

    # Push to Named XCom
    ti.xcom_push(key="extracted_rates", value=raw_data)
    logging.info("Pushed raw data to named XCom key: 'extracted_rates'")


def transform_exchange_rate_data(ti, **kwargs):
    """
    Task 2: Transform Exchange Rate Data
    Pulls raw data from Task 1 via Named XCom, transforms it to the required schema,
    and pushes transformed data via Named XCom.
    """
    raw_data = ti.xcom_pull(task_ids="extract_currency_rates", key="extracted_rates")
    if not raw_data:
        raise ValueError("No data received from task 'extract_currency_rates' via XCom.")

    logging.info(f"Pulled {len(raw_data)} items from named XCom 'extracted_rates'")

    # Current UTC timestamp for retrieved_at
    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    transformed_records = []
    for item in raw_data:
        transformed = {
            "base_currency": item.get("base", "EUR"),
            "target_currency": item.get("quote"),
            "exchange_rate": float(item.get("rate")),
            "rate_date": item.get("date"),
            "source": "Frankfurter API",
            "retrieved_at": retrieved_at,
        }
        transformed_records.append(transformed)
        logging.info(f"Transformed record: {transformed}")

    # Push to Named XCom
    ti.xcom_push(key="transformed_rates", value=transformed_records)
    logging.info(f"Pushed {len(transformed_records)} transformed records to named XCom key: 'transformed_rates'")


def load_data_into_mongodb(ti, **kwargs):
    """
    Task 3: Load Data into MongoDB
    Pulls transformed data from Task 2 via Named XCom and inserts into MongoDB.
    """
    records = ti.xcom_pull(task_ids="transform_exchange_rates", key="transformed_rates")
    if not records:
        raise ValueError("No records received from task 'transform_exchange_rates' via XCom.")

    logging.info(f"Pulled {len(records)} records to load into MongoDB")
    logging.info(f"Connecting to MongoDB at {MONGO_URI} (Database: {DB_NAME}, Collection: {COLLECTION_NAME})")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    result = collection.insert_many(records)
    logging.info(f"Successfully inserted {len(result.inserted_ids)} records into MongoDB. IDs: {result.inserted_ids}")

    # Print out all stored records for verification in logs
    total_docs = collection.count_documents({})
    logging.info(f"Total documents currently in '{COLLECTION_NAME}': {total_docs}")
    client.close()


with DAG(
    dag_id="currency_exchange_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for daily Frankfurter currency exchange rates into MongoDB",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ETL", "Currency", "MongoDB", "Frankfurter"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_currency_rates",
        python_callable=extract_currency_exchange_rates,
    )

    transform_task = PythonOperator(
        task_id="transform_exchange_rates",
        python_callable=transform_exchange_rate_data,
    )

    load_task = PythonOperator(
        task_id="load_data_to_mongodb",
        python_callable=load_data_into_mongodb,
    )

    extract_task >> transform_task >> load_task

