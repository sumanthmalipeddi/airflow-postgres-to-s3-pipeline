from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils import timezone
from datetime import datetime, timedelta
import pandas as pd
import requests
import csv
from custom_operator.postgres_to_s3_operator import PostgresToS3Operator

#default args
default_args = {
    "owner" : "sumanth",
    "depends_on_past" : False,
    "start_date" : datetime(2025, 2, 2),
}

dag = DAG(
    dag_id = "airbnb_postgres_to_s3",
    default_args=default_args,
    description="Download, preprocess and load data from Postgres to S3.",
    schedule= timedelta(days=1),
    catchup= False,
)

listing_dates = ["2025-11-07", "2025-10-05", "2025-09-06","2025-08-04","2025-07-04","2025-06-09","2025-05-02","2025-04-03","2025-03-02","2025-02-06","2025-01-05"]
#listing_dates = ["2025-11-07"]

def download_csv():
    listing_url_template = "https://data.insideairbnb.com/united-states/ny/albany/{date}/visualisations/listings.csv"
    for date in listing_dates:
        url = listing_url_template.format(date=date)
        response = requests.get(url)
        if response.status_code == 200:
            with open(f"/tmp/airbnbdata/listing-{date}.csv","wb") as f:
                f.write(response.content)
        else:
            print(f"faield to download {url}")

def preprocess_csv():
    for date in listing_dates:
        input_file = f"/tmp/airbnbdata/listing-{date}.csv"
        output_file = f"/tmp/airbnbdata/listing-{date}-processed.csv"
        df = pd.read_csv(input_file)
        #df.fillna('',inplace=True)
        df.to_csv(output_file,index=False,na_rep='\\N',quoting=csv.QUOTE_MINIMAL)

 
create_table = SQLExecuteQueryOperator(
    task_id = "create_table",
    conn_id= "airbnb_postgres",
    sql="""
    DROP TABLE IF EXISTS listings;
    CREATE TABLE IF NOT EXISTS listings (
            id BIGINT,
            name TEXT,
            host_id INTEGER,
            host_name VARCHAR(255),
            neighbourhood_group VARCHAR(255),
            neighbourhood VARCHAR(255),
            latitude DECIMAL(10, 7),
            longitude DECIMAL(10, 7),
            room_type VARCHAR(50),
            price NUMERIC(10,2),
            minimum_nights INTEGER,
            number_of_reviews INTEGER,
            last_review DATE,
            reviews_per_month NUMERIC(10, 2),
            calculated_host_listings_count INTEGER,
            availability_365 INTEGER,
            number_of_reviews_ltm INTEGER,
            license VARCHAR(255),
            load_date DATE DEFAULT CURRENT_DATE,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    dag=dag
)

def load_csv_to_postgres():
    hook = PostgresHook(postgres_conn_id = "airbnb_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        "DELETE FROM listings WHERE load_date = CURRENT_DATE;"
    )
    conn.commit()

    for date in listing_dates:
        processed_file = f"/tmp/airbnbdata/listing-{date}-processed.csv"
        with open(processed_file, 'r') as f:
            cur.copy_expert(
                r"""
                COPY listings (
                    id,
                    name,
                    host_id,
                    host_name,
                    neighbourhood_group,
                    neighbourhood,
                    latitude,
                    longitude,
                    room_type,
                    price,
                    minimum_nights,
                    number_of_reviews,
                    last_review,
                    reviews_per_month,
                    calculated_host_listings_count,
                    availability_365,
                    number_of_reviews_ltm,
                    license
                )
                FROM STDIN
                WITH (
                    FORMAT CSV,
                    HEADER TRUE,
                    NULL '\N'
                );
                """, 
                f,
                )
    conn.commit()
    cur.close()
    conn.close()

download_csv_task = PythonOperator(
    task_id = "download_csv",
    python_callable= download_csv,
    dag = dag
)

preprocess_csv_task = PythonOperator(
    task_id = "preprocess_csv_task",
    python_callable= preprocess_csv
)   

load_csv_task = PythonOperator(
    task_id = "load_csv_to_postgres",
    python_callable= load_csv_to_postgres,
    dag = dag
)

transfer_postgres_to_s3 = PostgresToS3Operator(
    task_id = "transfer_postgres_to_s3r",
    postgres_conn_id="airbnb_postgres",
    query= "SELECT * FROM listings where load_date = CURRENT_DATE",
    s3_bucket= "aws-airbnb-s3bucket",
    s3_conn_id= "aws_s3_airbnb",
    s3_key="airbnb-test/postgres_data_{{ ds }}.csv",
    dag=dag
)


download_csv_task >> preprocess_csv_task >> create_table >> load_csv_task >> transfer_postgres_to_s3