import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

CSV_FILE = os.getenv("CSV_FILE", "Sales_DataSet.csv")

def extract_data() -> pd.DataFrame:
    df = pd.read_csv(CSV_FILE)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df.drop_duplicates(inplace=True)
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
    return df
