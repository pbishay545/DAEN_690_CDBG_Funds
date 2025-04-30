import psycopg2
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta


db_params = {
    "host": "database-1.c7goeg04uy2n.us-east-1.rds.amazonaws.com",
    "port": 5432,
    "user": "CDBG_DB",
    "password": "cdbgcdbg",
    "dbname": "postgres"
}

api_url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
table_name = "hud_awards_puerto_rico"
date_file = "last_query_date.csv"


def get_last_query_date():
    if os.path.exists(date_file):
        df = pd.read_csv(date_file)
        return df['last_query_date'].iloc[0]
    else:
        return "2000-01-01"  # Default fallback


def save_last_query_date(date_str):
    pd.DataFrame([{"last_query_date": date_str}]).to_csv(date_file, index=False)


def connect_postgres():
    return psycopg2.connect(**db_params)


def get_existing_df(conn, table_name):
    try:
        return pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
    except Exception:
        return pd.DataFrame()  


def push_to_postgres(df, table_name):
    conn = connect_postgres()
    cursor = conn.cursor()
    
    if df.empty:
        print("No new data to push.")
        return
    

    columns = ', '.join([f'"{col}" TEXT' for col in df.columns])
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            {columns}
        );
    """)
    

    for _, row in df.iterrows():
        values = tuple(str(val).replace("'", "''") if pd.notnull(val) else None for val in row)
        placeholders = ', '.join(['%s'] * len(values))
        cursor.execute(f'INSERT INTO "{table_name}" VALUES ({placeholders});', values)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Pushed {len(df)} new rows to PostgreSQL.")


last_date = get_last_query_date()


headers = {"Content-Type": "application/json"}
payload = {
    "limit": 1000,
    "page": 1,
    "filters": {
        "award_type_codes": ["02", "03", "04", "05"],
        "agencies": [{"type": "awarding", "tier": "toptier", "name": "Department of Housing and Urban Development"}],
        "place_of_performance_locations": [{"country": "USA", "state": "PR"}],
        "date_type": "action_date",
        "date_range": {"start_date": last_date, "end_date": datetime.today().strftime("%Y-%m-%d")}
    },
    "fields": [
        "Award ID", "Recipient Name", "Start Date", "End Date", "Award Amount",
        "Awarding Agency", "Awarding Sub Agency", "Contract Award Type",
        "Award Type", "Funding Agency", "Funding Sub Agency", "generated_internal_id"
    ]
}

response = requests.post(api_url, headers=headers, data=json.dumps(payload))
if response.status_code != 200:
    raise RuntimeError(f"API Error: {response.status_code} - {response.text}")

results = response.json().get("results", [])
if not results:
    print("No new data found.")
    exit()

new_df = pd.DataFrame(results)


conn = connect_postgres()
existing_df = get_existing_df(conn, table_name)

if not existing_df.empty:
    new_df = new_df[~new_df['generated_internal_id'].isin(existing_df['generated_internal_id'])]


push_to_postgres(new_df, table_name)

save_last_query_date(datetime.today().strftime("%Y-%m-%d"))