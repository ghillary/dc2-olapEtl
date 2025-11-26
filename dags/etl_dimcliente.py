from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import os


CSV_PATH = "/opt/airflow/csvs"

engine_dw = create_engine("postgresql+psycopg2://dw_user:dw_pass@postgres_dw:5432/adworks_dw")

def load_dimcliente():
    
    customer      = pd.read_csv(os.path.join(CSV_PATH, "Customer.csv"))
    person        = pd.read_csv(os.path.join(CSV_PATH, "Person.csv"))
    address       = pd.read_csv(os.path.join(CSV_PATH, "Address.csv"))
    be_address    = pd.read_csv(os.path.join(CSV_PATH, "BusinessEntityAddress.csv"))
    stateprov     = pd.read_csv(os.path.join(CSV_PATH, "StateProvince.csv"))
    countryregion = pd.read_csv(os.path.join(CSV_PATH, "CountryRegion.csv"))

    

    
    df = customer.merge(person, left_on="PersonID", right_on="BusinessEntityID")

   
    df = df.merge(
        be_address,
        left_on="PersonID",
        right_on="BusinessEntityID",
        how="left"
    ).merge(
        address,
        left_on="AddressID",
        right_on="AddressID",
        how="left"
    )

    
    df = df.merge(
        stateprov,
        left_on="StateProvinceID",
        right_on="StateProvinceID",
        how="left"
    ).merge(
        countryregion,
        left_on="CountryRegionCode",
        right_on="CountryRegionCode",
        how="left"
    )

    
    df_final = df[[
        "CustomerID",
        "FirstName",
        "LastName",
        "City",
        "Name_x",   
        "Name_y"   
    ]].copy()

    df_final["FullName"] = df_final["FirstName"] + " " + df_final["LastName"]

    df_final.rename(columns={
        "CustomerID": "customerkey",
        "City": "city",
        "Name_x": "stateprovince",
        "Name_y": "countryregion"
    }, inplace=True)

    df_final.to_sql("dimcliente", engine_dw, if_exists="append", index=False)


dag = DAG(
    "etl_dimcliente",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False
)

task = PythonOperator(
    task_id="load_dimcliente",
    python_callable=load_dimcliente,
    dag=dag
)
