from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

CSV = "/opt/airflow/csvs"
engine_dw = create_engine("postgresql+psycopg2://dw_user:dw_pass@postgres_dw:5432/adworks_dw")

def load_dimproduto():
    product      = pd.read_csv(f"{CSV}/Product.csv")
    subcat       = pd.read_csv(f"{CSV}/ProductSubcategory.csv")
    category     = pd.read_csv(f"{CSV}/ProductCategory.csv")

    df = product.merge(subcat, on="ProductSubcategoryID", how="left")
    df = df.merge(category, on="ProductCategoryID", how="left")

    df_final = df[[
        "ProductID",
        "Name_x",
        "ProductNumber",
        "Color",
        "Name_y",
        "Name"
    ]]

    df_final.columns = [
        "productkey",
        "productname",
        "productnumber",
        "color",
        "subcategory",
        "category"
    ]

    df_final.to_sql("dimproduto", engine_dw, if_exists="append", index=False)

dag = DAG(
    "etl_dimproduto",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False
)

PythonOperator(task_id="load_dimproduto", python_callable=load_dimproduto, dag=dag)
