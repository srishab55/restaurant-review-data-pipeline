#  Restaurant & Reviews Data Pipeline

This repository contains two modular data pipelines for processing restaurant and review data using Apache Spark, MySQL, and Airflow.

## Project Structure

```project/
├── adhoc_scripts
│   ├── json_chunks_to_parquet.py
│   ├── json_to_chunks.py
│   └── sqlite_to_mysql_dump.py
├── config
│   ├── __init__.py
│   ├── config.py
│   └── db_config.py
├── dags
│   ├── restaurant_etl_dag.py
│   └── reviews_sql_data_dag.py
├── docker-compose.yml
├── init
│   ├── 01-create-reviews-table.sql
│   └── 01-init-user.sql
├── metrics
├── restaurant_pipeline.sh
├── setup.sh
├── src
│   ├── __init__.py
│   ├── config_loader.py
│   ├── constants.py
│   ├── ingestion
│   │   ├── __init__.py
│   │   └── mysql_to_local_dump.py
│   ├── pipeline
│   │   ├── __init__.py
│   │   └── run_pipeline.py
│   └── transformation
│       ├── __init__.py
│       ├── transform_restaurants.py
│       └── transform_reviews.py
├── tests
│   ├── __init__.py
│   ├── test_transform_restaurants.py
│   └── test_transform_reviews.py
├── warehouse
│   ├── restaurants.ddl.sql
│   ├── restaurants.insert.sql
│   ├── reviews.ddl.sql
│   ├── reviews.insert.sql
│   └── schema.sql
└── webserver_config.py
```

## Prerequisites

- Python 3.8+
- Apache Spark
- Apache Airflow
- MySQL (with review data pre-loaded)

## Pipeline 1: Step-by-Step Pipeline Execution (Restaurant Pipeline via Shell Script)

### 1. Create a Python Virtual Environment

```bash
python -m venv venv
source venv/bin/activate
```
### 2. Install requirements 

```cd ~/restaurant_data_pipeline/
pip install -r requirements.txt
```

### 3. Download the dataset

```#!/bin/bash
curl -L -o ~/project_directory/restaurant_data_pipeline/data/raw/swiggy-restaurants-dataset.zip\
  https://www.kaggle.com/api/v1/datasets/download/ashishjangra27/swiggy-restaurants-dataset
```
### 4. Run full Pipeline on the dataset

```bash restaurant_pipeline.sh```

### 5. Add cron job for scheduling
```
0 2 * * * /full/path/to/run_reviews_pipeline.sh >> /full/path/to/logs/pipeline.log 2>&1
```

## - Pipeline 2 : Step-by-Step Pipeline Execution (Reviews Pipeline via Airflow)

### 1. Create a Python Virtual Environment

```bash
python -m venv venv
source venv/bin/activate
```

### 2. Install requirements 
```
cd ~/restaurant_data_pipeline/
pip install -r requirements.txt

```
### 3. Download the dataset
```
#!/bin/bash
curl -L -o ~/restaurant_data_pipeline/data/raw/data.zip\
  https://www.kaggle.com/api/v1/datasets/download/ajaysh/amazon-fine-food-reviews
```
### 4. Download the dataset
```
docker-compose up -d
```
### 5. Run One time script to load data into MYsql Server
```
python adhoc_scripts/sqlite_to_mysql.py
```
### 6. Trigger Dag for generating reviews data as a stream

Go to UI and hit that play button

### 7. Run tranformation and data cleaning script
```
python src/transformation/transform_reviews.py
```



## Pipeline Design

This project comprises two separate ETL pipelines built to process **Restaurant Metadata** and **Restaurant Reviews**. Both pipelines are structured with modularity and scalability in mind, allowing for better maintenance, easier debugging, and future expansion.

### Architecture Overview

- **Extraction Layer**: 
  - For the **restaurant pipeline**, large static JSON files are read and chunked to handle memory limitations on local machines.
  - For the **reviews pipeline**, data is initially extracted from a SQLite database and loaded into a MySQL database using a one-time script. Airflow then simulates real-time extraction by querying MySQL and writing batches to disk.

- **Transformation Layer**:
  - Applies data cleaning, normalization, formatting, and structuring.
  - For both pipelines, PySpark jobs handle complex transformations, including null handling, text normalization, data type parsing, and field standardization.

- **Load Layer**:
  - Cleaned and transformed data is written to Parquet format in a partitioned structure (e.g., `date={}/hr={}`) for downstream consumption.
  - The directory structure supports predicate pushdown and optimized querying in analytical tools or data warehouses.

---

## Assumptions

- The restaurant data is large and requires chunked processing for execution as Json is less used for data pipelines and even if they are used it will have to be in smaller chunks otherwise the read overhead will exceed the memory.
- Reviews data is simulated to mimic streaming ingestion, it is assumed that the actual data will be in much larger size and pipeline is design accordingly.
- All data resides in local directories and are accessible and is used in batch mode to simulate streaming behavior.

---

## Performance Metrics

- **Chunked Processing**: Reduced memory overhead and improved runtime stability by splitting large JSON datasets into smaller, manageable chunks (~100MB each).
- **Partitioned Writes**: Writing data with partitioning (`date={}/hr={}`) improved query performance during downstream analysis.
- **Spark Config Optimizations**: Jobs tuned using configuration parameters such as dynamic allocation, executor memory limits, and batch size tuning.
- **Batch Ingestion**: Reviews ingestion was handled in batches of 10,000 rows to optimize MySQL-to-Disk simulation latency.

---

## Error Handling

- **Try-Except Blocks**: Wrapped critical operations such as database connections, file reads, and writes with error handling to avoid pipeline crashes.
- **Logging**: Enabled structured logging throughout scripts to capture warnings, errors, and processing status. Helpful in debugging and monitoring.
- **Graceful Shutdown**: Resources like database cursors and connections are closed in `finally` blocks to ensure cleanup after failures.
- **Airflow Alerts** (Not executed here): Airflow DAGs can be extended with email or Slack notifications on task failures for better monitoring in production environments.


## Potential Improvements

- **True Real-Time Streaming**: Integrating Kafka or other streaming platforms would allow for genuine real-time ingestion and processing.
- **Schema Registry**: Schema evolution handling is minimal. Integrating tools like Apache Avro or a schema registry could enforce schema consistency.
- **Data Quality Checks**: Currently, there are no explicit data validation rules or metrics tracking. Incorporating tools like Great Expectations or custom data validation frameworks would improve pipeline robustness.
- **Orchestration Abstraction**: A unified DAG for both pipelines could be created with better modular operators and conditional task execution.
- **Dockerization & CI/CD**: While Docker is used for services like MySQL and Airflow, a fully containerized execution pipeline and CI/CD integration would make deployment more seamless.

---
