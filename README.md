# 🍽️ Restaurant & Reviews Data Pipeline

This repository contains two modular data pipelines for processing restaurant and review data using Apache Spark, MySQL, and Airflow.

## 📁 Project Structure

```project/
│
├── dags/                            # Airflow DAGs
│   ├── restaurant_transformation_dag.py
│   └── reviews_ingestion_dag.py
│
├── src/
│   ├── transformation/transform_restaurants.py
│   └── ingestion/reviews_ingestion.py
│
├── scripts/
│   ├── json_to_chunks.py
│   └── json_chunks_to_parquet.py
│
├── config/
│   └── db_config.py                 # Optional DB config
│
├── data/
│   ├── raw/                         # Raw JSON and chunks
│   ├── processed/                   # Flattened restaurant parquet chunks
│   ├── interim/reviews/            # Dumped parquet files from reviews
│   ├── transformed/                # Cleaned restaurant output
│   └── temporary/last_processed.txt  # Tracks last timestamp for review ingestion
│
├── requirements.txt
├── .gitignore
└── README.md
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
