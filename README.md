# Daily Currency Exchange Rates ETL Pipeline with Apache Airflow & MongoDB

This project implements an automated, production-grade ETL (Extract, Transform, Load) pipeline using **Apache Airflow (via Astro CLI)**, **Frankfurter API**, **MongoDB**, and **Mongo Express**, running in containerized Docker environments.

---

## 🏗️ Architecture & Component Overview

```
+-----------------------------------------------------------------------------------+
|                                Docker Network                                     |
|                                                                                   |
|  +-----------------------------------------------------------------------------+  |
|  |                          Apache Airflow (Astro CLI)                         |  |
|  |                                                                             |  |
|  |  +-----------------------------+   Named XCom: 'extracted_rates'            |  |
|  |  | extract_currency_rates      | ------------------------------------+      |  |
|  |  +-----------------------------+                                     |      |  |
|  |                |                                                     v      |  |
|  |                |                   +----------------------------------+     |  |
|  |                +-----------------> | transform_exchange_rates         |     |  |
|  |                                    +----------------------------------+     |  |
|  |                                                     |                       |  |
|  |                                                     | Named XCom:           |  |
|  |                                                     | 'transformed_rates'   |  |
|  |                                                     v                       |  |
|  |  +-----------------------------+   loads data      +------------------+     |  |
|  |  | load_data_to_mongodb        | ----------------> | MongoDB Service  |     |  |
|  |  +-----------------------------+                   +------------------+     |  |
|  +-----------------------------------------------------------------------------+  |
|                                                                  ^                |
|                                                                  | reads / views  |
|                                                                  v                |
|                                                       +--------------------+      |
|                                                       | Mongo Express Web  |      |
|                                                       | (Port 8081)        |      |
|                                                       +--------------------+      |
+-----------------------------------------------------------------------------------+
```

### 1. Frankfurter API
- **Endpoint**: `https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY`
- **Source**: Free public currency exchange rate service tracking European Central Bank reference rates.

### 2. The 3 Airflow Pipeline Tasks
- **Task 1: `extract_currency_rates`**: Fetches real-time daily currency rates for EUR against USD, GBP, and JPY from the Frankfurter API and pushes the raw response to Airflow **Named XCom** (`extracted_rates`).
- **Task 2: `transform_exchange_rates`**: Pulls raw rates from Named XCom, formats and cleans each item into standardized records with metadata, and pushes the clean records to Airflow **Named XCom** (`transformed_rates`).
  ```json
  {
    "base_currency": "EUR",
    "target_currency": "USD",
    "exchange_rate": 1.15,
    "rate_date": "2026-11-04",
    "source": "Frankfurter API",
    "retrieved_at": "2026-11-05T07:15:00"
  }
  ```
- **Task 3: `load_data_to_mongodb`**: Pulls transformed records from Named XCom and saves them into the `currency_exchange_db` database inside the `daily_rates` collection in MongoDB.

### 3. Docker Services & Meta Requirements
- **Docker Networking**: All services (Airflow Scheduler, Webserver, Triggerer, Postgres, MongoDB, and Mongo Express) are attached to the same Docker bridge network. Airflow securely communicates with MongoDB using service discovery at `mongodb:27017`.
- **Persistent Storage**: MongoDB uses a persistent Docker volume (`mongodb_data:/data/db`), preserving historical exchange rate records even when containers restart.
- **Mongo Express**: Configured as a web dashboard on port `8081` (`http://localhost:8081`) for inspecting MongoDB databases, collections, and documents without requiring CLI tools.

---

## 🚀 Getting Started

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)

### Running the Project
1. Start the pipeline and all services:
   ```bash
   astro dev start
   ```
2. Access the web interfaces:
   - **Airflow Web UI**: [http://localhost:8080](http://localhost:8080) (Default login: `admin` / `admin`)
   - **Mongo Express Web UI**: [http://localhost:8081](http://localhost:8081)

3. Trigger the DAG:
   - In Airflow UI, find `currency_exchange_etl_pipeline`.
   - Toggle it **ON** and click the **Trigger DAG** (Play) button.
   - Watch all 3 tasks turn dark green (Success).

4. Verify in Mongo Express:
   - Open [http://localhost:8081](http://localhost:8081).
   - Click `currency_exchange_db` $\rightarrow$ `daily_rates`.
   - View your inserted currency rate documents.

---

## 📁 Repository Structure
```
├── dags/
│   └── currency_exchange_etl_dag.py   # Complete 3-task Airflow DAG with Named XComs
├── docker-compose.override.yml        # MongoDB & Mongo Express services with volume & network
├── requirements.txt                   # pymongo and requests dependencies
├── .astro/
│   └── config.yaml                    # Astro project configuration
├── Dockerfile                         # Astronomer runtime image definition
└── README.md                          # Project documentation and run guide
```
