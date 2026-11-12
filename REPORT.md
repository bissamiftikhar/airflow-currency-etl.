# Building an Airflow ETL Pipeline for Daily Currency Exchange Rates

**Course Assignment:** Task 3 – Building Airflow ETL Pipeline  
**Author:** Bissam Iftikhar  
**Repository:** [https://github.com/bissamiftikhar/airflow-currency-etl.](https://github.com/bissamiftikhar/airflow-currency-etl.)  
**Tech Stack:** Apache Airflow (Astro CLI), Docker & Docker Compose, MongoDB, Mongo Express, Frankfurter API, Python (Requests, PyMongo)

---

## 1. Project Overview & Objectives

The goal of this assignment is to build and orchestrate an end-to-end automated ETL (Extract, Transform, Load) data pipeline using Apache Airflow managed via Astronomer (Astro CLI). 

The pipeline performs the following core functions:
1. **Extracts** live daily currency exchange rates from the public **Frankfurter API** for EUR against USD, GBP, and JPY.
2. **Transforms** the raw API response into standardized JSON records containing required metadata (source, base currency, target currency, rate date, and UTC retrieval timestamp).
3. **Loads** the cleaned records into a persistent **MongoDB** database.
4. Uses **Mongo Express** as a web-based administrative UI to inspect and verify stored records.
5. Uses **Named Airflow XComs** to pass data between pipeline stages.

---

## 2. Docker Architecture & Container Networking

All components run as Docker services orchestrated via Astro CLI and `docker-compose.override.yml`. 

Key configuration features:
- **Docker Networking:** MongoDB and Mongo Express join the shared `airflow` bridge network. Airflow containers communicate directly with MongoDB using Docker internal DNS (`mongodb:27017`).
- **Persistent Storage:** MongoDB uses a named volume (`mongodb_data:/data/db`) to guarantee data persistence across container restarts.
- **Mongo Express:** Exposed on port `8081` for browser access.

### Docker Status & Running Containers
Below is the verification of the running Docker services:

![Docker ps](screenshots/Pasted%202026-11-12%20at%2010.55.55%20PM.png)
*Figure 1: Terminal output of `docker ps` showing all 7 running containers (Airflow scheduler, triggerer, api-server, dag-processor, postgres, mongodb, and mongo-express).*

![Docker Desktop](screenshots/Pasted%202026-11-12%20at%2010.55.45%20PM.png)
*Figure 2: Docker Desktop dashboard confirming active container health and resources.*

---

## 3. Airflow DAG Design & Code

The pipeline is implemented in `dags/currency_exchange_etl_dag.py` and scheduled `@daily`. It consists of 3 sequential tasks:

1. **`extract_currency_rates` (Extract):** Queries `https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY` and pushes raw data via Named XCom (`key="extracted_rates"`).
2. **`transform_exchange_rates` (Transform):** Pulls raw data from Named XCom, converts each item into the required schema, and pushes clean records via Named XCom (`key="transformed_rates"`).
3. **`load_data_to_mongodb` (Load):** Pulls transformed records from Named XCom and performs an `insert_many` operation into `currency_exchange_db.daily_rates` in MongoDB.

### Required Document Schema:
```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.1122,
  "rate_date": "2026-11-12",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-12T17:45:52"
}
```

### Source Code View:
![DAG Code](screenshots/Pasted%202026-11-12%20at%2010.56.39%20PM.png)
*Figure 3: Source code implementation in the development editor.*

---

## 4. Named XCom Data Transfer

The assignment requires using Airflow Named XComs rather than implicit return values. 

The Airflow UI confirmed that custom named keys (`extracted_rates` and `transformed_rates`) were registered and read across tasks:

![Named XCom](screenshots/Pasted%202026-11-12%20at%2010.53.39%20PM.png)
*Figure 4: Airflow XCom Storage tab verifying key `transformed_rates` holding the transformed records.*

---

## 5. Pipeline Execution & Airflow Web UI Results

The DAG was triggered in Airflow and ran with zero errors. All three tasks completed in `success` state.

![Airflow Graph View](screenshots/Pasted%202026-11-12%20at%2010.49.00%20PM.png)
*Figure 5: Airflow Graph View showing the sequential execution `extract` -> `transform` -> `load` with green checkmarks.*

![Airflow Overview](screenshots/Pasted%202026-11-12%20at%2010.49.11%20PM.png)
*Figure 6: Airflow DAG Overview showing all 3 tasks successful (0 Failed Tasks, 0 Failed Runs).*

---

## 6. Database Verification in Mongo Express

Connecting to Mongo Express at `http://localhost:8081` verified that the exchange rates were properly stored in the `currency_exchange_db` database and `daily_rates` collection:

![Mongo Express Document](screenshots/Pasted%202026-11-12%20at%2010.54.07%20PM.png)
*Figure 7: Stored currency exchange rate document in Mongo Express showing complete schema and values.*

---

## 7. Submission Link & Conclusion

- **GitHub Repository:** [https://github.com/bissamiftikhar/airflow-currency-etl.](https://github.com/bissamiftikhar/airflow-currency-etl.)
- **Word Document Report:** [ETL_Pipeline_Assignment_Report.docx](ETL_Pipeline_Assignment_Report.docx)

The pipeline completely fulfills all requirements specified in the assignment brief, demonstrating end-to-end containerized ETL orchestration with Astro CLI, Apache Airflow, Docker, and MongoDB.
