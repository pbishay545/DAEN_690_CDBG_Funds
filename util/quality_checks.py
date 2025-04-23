


import pandas as pd
import numpy as np
import re
import psycopg2


# Database connection parameters
db_params = {
    "host": "database-1.c7goeg04uy2n.us-east-1.rds.amazonaws.com",
    "port": 5432,
    "user": "CDBG_DB",
    "password": "cdbgcdbg",
    "dbname": "postgres"
}

# Connect to PostgreSQL
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Fetch all table names
cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
tables = cursor.fetchall()

# Read each table into a pandas DataFrame
dataframes = {}
for table in tables:
    table_name = table[0]
    df = pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
    dataframes[table_name] = df

# Close the connection
cursor.close()
conn.close()

# Now, `dataframes` is a dictionary where keys are table names and values are DataFrames
print(dataframes.keys())  # Print loaded table names

table_info = {}

for table_name, df in dataframes.items():
    row_count = len(df)  # Number of rows in the DataFrame
    memory_usage = df.memory_usage(deep=True).sum()  # Total memory usage in bytes
    table_info[table_name] = {
        'row_count': row_count,
        'memory_usage_bytes': memory_usage,
        'memory_usage_mb': memory_usage / (1024 ** 2)  # Convert to MB
    }



def describe_completeness(df):
    total_cells = df.size
    missing_cells = df.isnull().sum().sum()
    missing_pct = (missing_cells / total_cells) * 100
    print(f"[Completeness] Total cells: {total_cells}, Missing: {missing_cells} ({missing_pct:.2f}%)")

def describe_uniqueness(df):
    total_rows = len(df)
    duplicate_rows = df.duplicated().sum()
    duplicate_pct = (duplicate_rows / total_rows) * 100
    print(f"[Uniqueness] Total rows: {total_rows}, Duplicates: {duplicate_rows} ({duplicate_pct:.2f}%)")

def describe_accuracy(df):
    numeric_cols = df.select_dtypes(include=np.number)
    if numeric_cols.empty:
        print("[Accuracy] No numeric columns to assess.")
        return
    for col in numeric_cols:
        values = df[col].dropna()
        if len(values) == 0:
            continue
        z_scores = np.abs((values - values.mean()) / values.std())
        outliers = (z_scores > 3).sum()
        outlier_pct = (outliers / len(values)) * 100
        print(f"[Accuracy] Column '{col}': {outliers} outliers out of {len(values)} values ({outlier_pct:.2f}%)")

def describe_atomicity(df):
    for col in df.columns:
        if df[col].dtype == object or df[col].dtype == "string":
            try:
                multi_val_pct = df[col].dropna().astype(str).str.contains(r'[;,|]').mean() * 100
                if multi_val_pct > 0:
                    print(f"[Atomicity] Column '{col}': {multi_val_pct:.2f}% rows contain multiple values")
            except Exception as e:
                print(f"[Atomicity] Column '{col}': Error assessing — {e}")

def describe_conformity(df):
    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna().astype(str)
            if sample.empty:
                continue
            format_sample = sample[:10]
            print(f"[Conformity] Sample values from '{col}':")
            for val in format_sample:
                print(f"    - {val}")
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            print(f"[Conformity] Column '{col}' has datetime format.")

def describe_dataset(df, name="DataFrame"):
    print(f"\n========== Dataset Description: {name} ==========")
    describe_completeness(df)
    describe_uniqueness(df)
    describe_accuracy(df)
    describe_atomicity(df)
    describe_conformity(df)
    print("==================================================\n")
    
name = "program_partners"

df = dataframes[name]

describe_dataset(df, name = name)
