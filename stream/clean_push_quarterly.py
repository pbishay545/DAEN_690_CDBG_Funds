import pandas as pd
import re
import os

import psycopg2
import pandas as pd

# Database connection parameters
db_params = {
    "host": "POSTGRES ENDPOINT",
    "port": "PORT",
    "user": "CDBG_DB",
    "password": "PASSWORD",
    "dbname": "DATABASE"
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

path = r"results"
base_path = r"results"
sub_path = r"clean_results"

# List of files in the directory
array = os.listdir(path)

columns_to_clean = [
    "Most Impacted and Distressed Expended",
    "Total Projected Budget from All Sources",
    "Total Budget",
    "Total Obligated",
    "Total Funds Drawdown",
    "Program Funds Drawdown",
    "Program Income Drawdown",
    "Program Income Received",
    "Total Funds Expended"
]

new_column_names = {
    "Grantee Activity Number": "grantee_activity_number",
    "Activity Title": "activity_title",
    "Activity Type": "activity_type",
    "Activity Status": "activity_status",
    "Project Number": "project_number",
    "Project Title": "project_title",
    "Projected Start Date": "projected_start_date",
    "Projected End Date": "projected_end_date",
    "Benefit Type": "benefit_type",
    "National Objective": "national_objective",
    "Responsible Organization": "responsible_organization",
    "Program Income Account": "program_income_account",
    "Activity Description": "activity_description",
    "Location Description": "location_description",
    "Activity Progress Narrative": "activity_progress_narrative",
    "Most Impacted and Distressed Expended_pre": "most_impacted_and_distressed_expended_pre",
    "Most Impacted and Distressed Expended_post": "most_impacted_and_distressed_expended_post",
    "Total Projected Budget from All Sources_pre": "total_projected_budget_from_all_sources_pre",
    "Total Projected Budget from All Sources_post": "total_projected_budget_from_all_sources_post",
    "Total Budget_pre": "total_budget_pre",
    "Total Budget_post": "total_budget_post",
    "Total Obligated_pre": "total_obligated_pre",
    "Total Obligated_post": "total_obligated_post",
    "Total Funds Drawdown_pre": "total_funds_drawdown_pre",
    "Total Funds Drawdown_post": "total_funds_drawdown_post",
    "Program Funds Drawdown_pre": "program_funds_drawdown_pre",
    "Program Funds Drawdown_post": "program_funds_drawdown_post",
    "Program Income Drawdown_pre": "program_income_drawdown_pre",
    "Program Income Drawdown_post": "program_income_drawdown_post",
    "Program Income Received_pre": "program_income_received_pre",
    "Program Income Received_post": "program_income_received_post",
    "Total Funds Expended_pre": "total_funds_expended_pre",
    "Total Funds Expended_post": "total_funds_expended_post"
}

def clean_value(val):
    if isinstance(val, str):
        val = re.sub(r'[sSoO]', '', val)  
        val = val.replace(',', '')  
        val = val.replace('(', '-').replace(')', '')  
        try:
            return float(val)
        except ValueError:
            return None  # Return None if conversion fails
    return val

# List to store DataFrames
all_data_frames = []

# Process each file in the directory
for table in array:
    path_to_file = os.path.join(base_path, table)

    # Read the CSV
    df = pd.read_csv(path_to_file)
    print(f"Processing: {table}")
    
    # Process each target column
    for col in columns_to_clean:
        if col in df.columns:
            # Apply the cleaning function to split the values into _pre and _post columns
            df[[col + "_pre", col + "_post"]] = df[col].apply(
                lambda x: pd.Series([clean_value(i) for i in eval(x)])  
            )
            df.drop(columns=[col], inplace=True)  # Drop the original column
    
    # Consolidate 'Location Description' and 'Locatlon DescrIptlon'
    df['Location Description'] = df['Location Description'].fillna(df['Locatlon DescrIptlon'])
    
    # Consolidate 'Activity Description' and 'Actlvlty Descrlpton:'
    df['Activity Description'] = df['Activity Description'].fillna(df['Actlvlty Descrlpton:'])
    
    # Drop the old columns
    df = df.drop(columns=['Locatlon DescrIptlon', 'Actlvlty Descrlpton:'])
    

    df.rename(columns=new_column_names, inplace=True)
    

    all_data_frames.append(df)



from psycopg2.extras import execute_values
import psycopg2
import pandas as pd


def check_for_new_data(existing_df, new_data_path, unique_column):
    new_df = pd.read_csv(new_data_path)
    new_df.columns = existing_df.columns t
    

    existing_bid_names = existing_df[unique_column].unique()

    new_rows = new_df[~new_df[unique_column].isin(existing_bid_names)]

    return new_rows

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
        

final_df = pd.concat(all_data_frames, ignore_index=True)


final_output_path = os.path.join(sub_path, "cleaned_combined_data.csv")
final_df


new_rows = check_for_new_data(dataframes["stage_4_quarterly_contracts"],"cleaned_combined_data.csv", "grantee_activity_number")
print(new_rows)
push_updates_to_postgres(new_rows, "bids", db_params) 