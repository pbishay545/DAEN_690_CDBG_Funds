import requests
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import os


LAST_PULL_FILE = "last_puerto_rico_disaster_pull.csv"
DATA_FILE = "puerto_rico_disasters_1950_2025.csv"
TABLE_NAME = "puerto_rico_disasters"

db_params = {
    "host": "POSTGRES ENDPOINT",
    "port": "PORT",
    "user": "CDBG_DB",
    "password": "PASSWORD",
    "dbname": "DATABASE"
}

def get_last_pull_date():
    if os.path.exists(LAST_PULL_FILE):
        df = pd.read_csv(LAST_PULL_FILE)
        return pd.to_datetime(df["last_pull"].iloc[0])
    return pd.to_datetime("1950-01-01")

def update_last_pull_date(new_date):
    df = pd.DataFrame({"last_pull": [new_date]})
    df.to_csv(LAST_PULL_FILE, index=False)

def fetch_puerto_rico_disasters(start_date):
    url = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"
    params = {
        "$filter": f"state eq 'PR' and declarationDate ge {start_date.strftime('%Y-%m-%dT%H:%M:%S')}Z",
        "$format": "json",
        "$top": 1000
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return pd.DataFrame(response.json()["DisasterDeclarationsSummaries"])

def push_updates_to_postgres(new_rows, table_name, db_params):
    if new_rows.empty:
        print(f"No new data to insert into {table_name}.")
        return

    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        data_tuples = list(new_rows.itertuples(index=False, name=None))
        columns = ", ".join(new_rows.columns)
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES %s"
        execute_values(cursor, insert_query, data_tuples)
        conn.commit()
        print(f"Inserted {len(data_tuples)} new rows into {table_name}.")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data into {table_name}: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    last_pull_date = get_last_pull_date()
    print(f"Fetching data from FEMA since {last_pull_date.date()}...")

    new_data = fetch_puerto_rico_disasters(last_pull_date)
    if new_data.empty:
        print("No new disaster records found.")
        return

 
    if os.path.exists(DATA_FILE):
        existing_df = pd.read_csv(DATA_FILE)
        combined_df = pd.concat([existing_df, new_data], ignore_index=True).drop_duplicates()
    else:
        combined_df = new_data
    combined_df.to_csv(DATA_FILE, index=False)


    push_updates_to_postgres(new_data, TABLE_NAME, db_params)


    max_date = pd.to_datetime(new_data["declarationDate"]).max()
    update_last_pull_date(max_date)
    print(f"Updated last pull date to {max_date.date()}.")

if __name__ == "__main__":
    main()
