from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

CSV = "/opt/airflow/csvs"
engine_dw = create_engine("postgresql+psycopg2://dw_user:dw_pass@postgres_dw:5432/adworks_dw")

def load_fatovendas():
    salesorderdetail = pd.read_csv(f"{CSV}/SalesOrderDetail.csv")
    salesorderheader = pd.read_csv(f"{CSV}/SalesOrderHeader.csv")

    df = salesorderdetail.merge(salesorderheader, on="SalesOrderID")

    df_final = pd.DataFrame({
        "datekey": pd.to_datetime(df["OrderDate"]).dt.strftime("%Y%m%d").astype(int),
        "productkey": df["ProductID"],
        "customerkey": df["CustomerID"],
        "orderquantity": df["OrderQty"],
        "salesamount": df["LineTotal"],
        "discountamount": df["UnitPriceDiscount"],
        "standardcost": df["UnitPrice"]
    })

    df_final.to_sql("fatovendas", engine_dw, if_exists="append", index=False)

dag = DAG(
    "etl_fatovendas",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False
)

PythonOperator(task_id="load_fatovendas", python_callable=load_fatovendas, dag=dag)
