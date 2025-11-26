from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from datetime import datetime

engine = create_engine("postgresql+psycopg2://dw_user:dw_pass@postgres_dw:5432/adworks_dw")

def create_tables():
    with engine.connect() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS dimtempo (
            datekey INT PRIMARY KEY,
            fulldate DATE,
            year INT,
            quarter INT,
            month INT,
            dayofweek INT
        );
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS dimproduto (
            productkey INT PRIMARY KEY,
            productname TEXT,
            productnumber TEXT,
            color TEXT,
            category TEXT,
            subcategory TEXT
        );
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS dimcliente (
            customerkey INT PRIMARY KEY,
            fullname TEXT,
            city TEXT,
            stateprovince TEXT,
            countryregion TEXT
        );
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS fatovendas (
            saleskey INT PRIMARY KEY,
            datekey INT,
            productkey INT,
            customerkey INT,
            orderquantity INT,
            salesamount NUMERIC,
            discountamount NUMERIC,
            standardcost NUMERIC
        );
        """)

dag = DAG(
    dag_id="create_dw_tables",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False
)

create_task = PythonOperator(
    task_id="create_tables",
    python_callable=create_tables,
    dag=dag
)
