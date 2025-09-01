import pandas as pd

PK_COLS = ["ordernumber", "orderlinenumber"]

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    
    num_int = ["ordernumber", "quantityordered", "orderlinenumber", "qtr_id", "month_id", "year_id"]
    for c in num_int:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

   
    num_float = ["priceeach", "sales", "msrp"]
    for c in num_float:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

   
    if "orderdate" in df.columns:
        df["orderdate"] = pd.to_datetime(df["orderdate"], errors="coerce").dt.date

   
    defaults = {
        "addressline2": "Ungiven Addressline2",
        "state": "Ungiven State",
        "territory": "Ungiven Territory",
    }
    for k, v in defaults.items():
        if k in df.columns:
            df[k] = df[k].fillna(v)

   
    for c in PK_COLS:
        if c not in df.columns:
            raise ValueError(f"Required PK column missing: {c}")

    
    df = df[df["ordernumber"].notna() & df["orderlinenumber"].notna()]
    df = df.drop_duplicates(subset=PK_COLS)

    return df
