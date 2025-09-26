from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, date
from fetch_news import fetch_news_data
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    'owner': 'growdataskills',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'newsapi_to_gcs',
    default_args=default_args,
    description='Fetch news articles and save as Parquet in GCS',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 12, 28),
    catchup=False,
)

def execute_snowflake_query(sql, **kwargs):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_test_conn")
    result = hook.run(sql)
    return result

fetch_news_data_task = PythonOperator(
    task_id='newsapi_data_to_gcs',
    python_callable=fetch_news_data,
    dag=dag,
)

snowflake_create_table_task = PythonOperator(
    task_id="snowflake_create_table",
    python_callable=execute_snowflake_query,
    op_kwargs={
        "sql": """CREATE TABLE IF NOT EXISTS news_api.PUBLIC.news_api_data USING TEMPLATE (
                    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                    FROM TABLE(INFER_SCHEMA (
                        LOCATION => '@news_api.PUBLIC.gcs_raw_data_stage',
                        FILE_FORMAT => 'parquet_format'
                    ))
                )"""
    },
    dag=dag,
)

snowflake_copy_task = PythonOperator(
    task_id="snowflake_copy_from_stage",
    python_callable=execute_snowflake_query,
    op_kwargs={
        "sql": """COPY INTO news_api.PUBLIC.news_api_data 
                  FROM @news_api.PUBLIC.gcs_raw_data_stage
                  MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE 
                  FILE_FORMAT = (FORMAT_NAME = 'parquet_format') 
               """
    },
    dag=dag,
)

news_summary_task = PythonOperator(
    task_id="create_or_replace_news_summary_tb",
    python_callable=execute_snowflake_query,
    op_kwargs={
        "sql":"""CREATE OR REPLACE TABLE news_api.PUBLIC.summary_news AS
        SELECT
            "source" AS news_source,
            COUNT(*) AS article_count,
            MAX("timestamp") AS latest_article_date,
            MIN("timestamp") AS earliest_article_date
        FROM news_api.PUBLIC.news_api_data as tb
        GROUP BY "source"
        ORDER BY article_count DESC;
    """
    },
    dag=dag,
)

author_activity_task = PythonOperator(
    task_id="create_or_replace_author_activity_tb",
    python_callable=execute_snowflake_query,
    op_kwargs={
       "sql":"""CREATE OR REPLACE TABLE news_api.PUBLIC.author_activity AS
        SELECT
            "author",
            COUNT(*) AS article_count,
            MAX("timestamp") AS latest_article_date,
            COUNT(DISTINCT "source") AS distinct_sources
        FROM news_api.PUBLIC.news_api_data as tb
        WHERE "author" IS NOT NULL
        GROUP BY "author"
        ORDER BY article_count DESC;
    """
    },
    dag=dag,
)

fetch_news_data_task >> snowflake_create_table_task >> snowflake_copy_task
snowflake_copy_task >> [news_summary_task, author_activity_task]