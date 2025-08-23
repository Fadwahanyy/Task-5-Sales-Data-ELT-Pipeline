import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv()

CSV_FILE   = os.getenv("CSV_FILE")
DB_USER    = os.getenv("DB_USER")
DB_PASS    = os.getenv("DB_PASS")
DB_NAME    = os.getenv("DB_NAME")
DB_HOST    = os.getenv("DB_HOST", "localhost")
DB_PORT    = int(os.getenv("DB_PORT", "5432"))
TABLE_NAME = os.getenv("TABLE_NAME", "sales_data")


url = URL.create(
    "postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)
engine = create_engine(url)

df = pd.read_csv(CSV_FILE)

df.columns = [c.strip().lower() for c in df.columns]

df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)

print(f"Inserted {len(df)} rows into {TABLE_NAME}")
