# NewsAPI-ETL-Pipeline-using-Airflow-GCS-Snowflake
## 🔎 Overview
  This project demonstrates how to design an event-driven data ingestion pipeline that extracts news data from the NewsAPI, stages data into Google Cloud Storage (GCS), and then incrementally ingests it into a     Snowflake target table using Apache Airflow (Google Cloud Composer) for orchestration.
  It simulates an industrial-grade ETL pipeline, combining modern cloud infrastructure, automation, and scalable data warehousing.

## 🛠️ Tech Stack
1. Python → API extraction & file writing
    
    
2. NewsAPI.org → Source of live news data
    
    
3. Google Cloud Storage (GCS) → Landing zone for raw data files
    
    
4. Snowflake → Data warehouse for storing and analyzing news data
    
    
5. Apache Airflow (Google Cloud Composer) → Workflow orchestration
    


## ⚙️ Architecture & Workflow
#### 1. Extract


* Python script calls the NewsAPI endpoint and extracts the latest news articles.


* The data is written into a new file in the assigned GCS bucket.


#### 2. Load (Staging)


  * A Snowflake Storage Integration is created to securely connect Snowflake with the GCS bucket.
  
  
  * An External Stage in Snowflake is used to read files from GCS.
  
  
#### 3. Transform & Load (Target Table)
  
  
  * Airflow DAG orchestrates the pipeline:
  
  
  * Calls the NewsAPI
  
  
  * Saves the file in GCS
  
  
  * Loads incremental data from the external stage into the Snowflake target table



## 🔑 Key Requirements for Success
#### 1. Connecting GCS Bucket ↔ Snowflake (Storage Integration)
   * Create a Snowflake Storage Integration.


  * Retrieve the Service Account by describing the integration.


  * Whitelist this Service Account in the GCS bucket permissions.


  * Set up the External Stage in Snowflake.


#### 2. Connecting Apache Airflow ↔ Snowflake
   * Use Google Composer (Airflow managed service) to orchestrate.


   * Install the Snowflake Airflow Provider Package in the Composer environment.


   * Add a Snowflake Connection in Airflow UI with all required credentials.



## 🚀 Setup & Execution Steps
### 1. Environment Setup
  * Create a Google Cloud Composer environment.


  * Add the Snowflake provider package to the environment dependencies.


### 2. Configure Airflow → Snowflake Connection
  * In the Airflow UI, create a new Connection of type Snowflake.


* Provide:


  - Account Name


    - Username / Password


    - Database / Schema


    - Warehouse


    - Role


#### 3. Storage Integration in Snowflake
      -- Create Storage Integration
      CREATE OR REPLACE STORAGE INTEGRATION gcs_snowflake_integration
        TYPE = EXTERNAL_STAGE
        STORAGE_PROVIDER = GCS
        ENABLED = TRUE
        STORAGE_ALLOWED_LOCATIONS = ('gcs://<your-bucket-name>');
      
      -- Get Service Account (to whitelist in GCS)
      DESC STORAGE INTEGRATION gcs_snowflake_integration;

#### 4. External Stage in Snowflake
      CREATE OR REPLACE STAGE news_stage
        URL = 'gcs://<your-bucket-name>/news-data/'
        STORAGE_INTEGRATION = gcs_snowflake_integration
        FILE_FORMAT = (TYPE = JSON);

#### 5. Airflow DAG
  - Place the DAG Python file inside the Composer environment’s dags/ folder (in the assigned GCS bucket).


- DAG Tasks:


  1. Extract data from NewsAPI


  2. Write JSON file to GCS


  3. Load data from External Stage → Snowflake Target Table



## 📂 Project Structure
news-data-pipeline/
│── dags/
│   └── news_etl_dag.py         # Airflow DAG
│── scripts/
│   └── extract_news.py         # Python script to fetch from NewsAPI
│── sql/
│   └── create_stage.sql        # External stage creation
│   └── create_table.sql        # Target Snowflake table schema
│── README.md                   # Project documentation


## 📈 Expected Outcome
A fully automated incremental ETL pipeline that fetches live news articles, lands them in GCS, and loads them into Snowflake for analysis.


Demonstrates cloud integration, orchestration, and industrial ETL best practices.



## 🌟 Learning Highlights
How to integrate API data extraction into a modern ETL workflow.


How to use Airflow DAGs for automation and scheduling.


How to securely connect Snowflake ↔ GCS with Storage Integrations.


How to design incremental data pipelines for analytics use cases.
