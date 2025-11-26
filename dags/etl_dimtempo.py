from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta

engine_dw = create_engine("postgresql+psycopg2://dw_user:dw_pass@postgres_dw:5432/adworks_dw")

def load_dimtempo():
    start = datetime(2010,1,1)
    end   = datetime(2030,12,31)

    dates = pd.date_range(start, end)

    df = pd.DataFrame({
        "datekey": dates.strftime("%Y%m%d").astype(int),
        "fulldate": dates,
        "year": dates.year,
        "quarter": dates.quarter,
        "month": dates.month,
        "dayofweek": dates.dayofweek
    })

    df.to_sql("dimtempo", engine_dw, if_exists="append", index=False)

dag = DAG(
    "etl_dimtempo",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False
)

PythonOperator(task_id="load_dimtempo", python_callable=load_dimtempo, dag=dag)
