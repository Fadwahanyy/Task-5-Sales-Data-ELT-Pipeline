from .extract import extract_data
from .transform import transform_data
from .load import load_data

def main():
    df = extract_data()
    df = transform_data(df)
    load_data(df)

if __name__ == "__main__":
    main()
