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

cd ~/restaurant_data_pipeline/
pip install -r requirements.txt

### 3. Download the dataset

```#!/bin/bash
curl -L -o ~/project_directory/restaurant_data_pipeline/data/raw/swiggy-restaurants-dataset.zip\
  https://www.kaggle.com/api/v1/datasets/download/ashishjangra27/swiggy-restaurants-dataset
```
### 4. Run full Pipeline on the dataset

```bash restaurant_pipeline.sh```


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
