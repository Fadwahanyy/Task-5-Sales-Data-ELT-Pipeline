import os
import pandas as pd
from sqlalchemy import text
from .db import get_engine

TABLE_NAME = os.getenv("TABLE_NAME", "sales_data")


FALLBACK_DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
  ordernumber INT,
  quantityordered INT,
  priceeach NUMERIC(10,2),
  orderlinenumber INT,
  sales NUMERIC(12,2),
  orderdate DATE,
  status VARCHAR(50),
  qtr_id INT,
  month_id INT,
  year_id INT,
  productline VARCHAR(100),
  msrp NUMERIC(10,2),
  productcode VARCHAR(50),
  customername VARCHAR(255),
  phone VARCHAR(50),
  addressline1 VARCHAR(255),
  addressline2 VARCHAR(255),
  city VARCHAR(100),
  state VARCHAR(100),
  postalcode VARCHAR(20),
  country VARCHAR(100),
  territory VARCHAR(50),
  contactlastname VARCHAR(100),
  contactfirstname VARCHAR(100),
  dealsize VARCHAR(20),
  PRIMARY KEY (ordernumber, orderlinenumber)
);
"""

def _create_table(engine):
   
    with engine.begin() as conn:
        conn.exec_driver_sql(FALLBACK_DDL)

def _filter_new_rows(engine, df: pd.DataFrame) -> pd.DataFrame:
    
    with engine.begin() as conn:
        existing = pd.read_sql(
            f"SELECT ordernumber, orderlinenumber FROM {TABLE_NAME}",
            conn
        )

    if existing.empty:
        return df

    existing["ordernumber"] = existing["ordernumber"].astype("Int64")
    existing["orderlinenumber"] = existing["orderlinenumber"].astype("Int64")

    merged = df.merge(
        existing.assign(_exists=1),
        on=["ordernumber", "orderlinenumber"],
        how="left"
    )
    return merged[merged["_exists"].isna()].drop(columns=["_exists"])

def load_data(df: pd.DataFrame):
    engine = get_engine()

    
    _create_table(engine)

   
    df_new = _filter_new_rows(engine, df)

   
    if not df_new.empty:
        df_new.to_sql(TABLE_NAME, engine, if_exists="append", index=False, method="multi", chunksize=500)

   
    with engine.connect() as conn:
        count_result = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}")).scalar()
        sample_result = conn.execute(text(f"SELECT * FROM {TABLE_NAME} LIMIT 10")).fetchall()

    print(f"Inserted: {len(df_new)} new rows")
    print(f"Row count in {TABLE_NAME}: {count_result}")
    for row in sample_result:
        print(row)
