import psycopg2
import pandas as pd

# Database connection parameters
db_params = {
    "host": "database-1.c7goeg04uy2n.us-east-1.rds.amazonaws.com",
    "port": 5432,
    "user": "CDBG_DB",
    "password": "cdbgcdbg",
    "dbname": "postgres"
}

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
tables = cursor.fetchall()


dataframes = {}
for table in tables:
    table_name = table[0]
    df = pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
    dataframes[table_name] = df


cursor.close()
conn.close()


print(dataframes.keys()) 

table_info = {}

for table_name, df in dataframes.items():
    row_count = len(df)  
    memory_usage = df.memory_usage(deep=True).sum()  
    table_info[table_name] = {
        'row_count': row_count,
        'memory_usage_bytes': memory_usage,
        'memory_usage_mb': memory_usage / (1024 ** 2) 

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd


options = webdriver.ChromeOptions()
options.add_argument("--headless")  # Run in headless mode (optional)
driver = webdriver.Chrome(options=options)


url = "https://recuperacion.pr.gov/en/procurement-and-nofa/procurement/"
driver.get(url)


wait = WebDriverWait(driver, 10)
wait.until(EC.presence_of_element_located((By.XPATH, "//table[contains(@id, 'wpdmmydls')]")))


data = []

def scrape_table():
    """Extracts procurement data from the table."""
    time.sleep(2)  # Allow time for data to load
    rows = driver.find_elements(By.XPATH, "//table[contains(@id, 'wpdmmydls')]//tbody/tr")
    
    print(f"Rows found on this page: {len(rows)}")  # Debugging line

    for row in rows:
        columns = row.find_elements(By.TAG_NAME, "td")
        if len(columns) > 5:  # Ensuring there are enough columns
            bid_name = columns[0].text.strip()
            bid_due_date = columns[1].text.strip()
            status = columns[2].text.strip()
            procuring_entity = columns[3].text.strip()
            service_type = columns[4].text.strip()
            details_link = columns[5].find_element(By.TAG_NAME, "a").get_attribute("href")
            
            data.append({
                "bid_name ": bid_name,
                "bid_due_date": bid_due_date,
                "status": status,
                "procuring_entity": procuring_entity,
                "service_type": service_type,
                "details_link": details_link
            })


scrape_table()


while True:
    try:
    
        active_page = driver.find_element(By.CSS_SELECTOR, "li.paginate_button.page-item.active").text.strip()
        print(f"Current Page: {active_page}")  
        
 
        next_button = driver.find_element(By.XPATH, "//a[contains(@class, 'page-link') and text()='Next']")
        
     
        driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
        time.sleep(1)  # Wait a bit after scrolling
        
  
        WebDriverWait(driver, 5).until(EC.element_to_be_clickable(next_button))
        

        try:
            next_button.click()
        except:
            driver.execute_script("arguments[0].click();", next_button)
        
        time.sleep(5)  
        
   
        scrape_table()
        
      
        new_active_page = driver.find_element(By.CSS_SELECTOR, "li.paginate_button.page-item.active").text.strip()
        
   
        if new_active_page == active_page:
            print("Pagination is stuck on the same page. Stopping.")
            break

    except Exception as e:
        print(f"Pagination ended or error occurred: {e}")
        break  


df = pd.DataFrame(data)

if not df.empty:
    df.to_csv("procurement_data.csv", index=False)
    print("Data scraped and saved to procurement_data.csv")
else:
    print("No data found. Check if table loads dynamically.")

# Close the browser
driver.quit()

from psycopg2.extras import execute_values
​
def check_for_new_data(existing_df, new_data_path, unique_column):
    new_df = pd.read_csv(new_data_path)
    new_df.columns = existing_df.columns t
    
​
    existing_bid_names = existing_df[unique_column].unique()
​
    new_rows = new_df[~new_df[unique_column].isin(existing_bid_names)]
​
    return new_rows
​
def push_updates_to_postgres(new_rows, table_name, db_params):
    if new_rows.empty:
        print(f"No new data to insert into {table_name}.")
        return
​
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
​
        data_tuples = list(new_rows.itertuples(index=False, name=None))
​
        columns = ", ".join(new_rows.columns)
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES %s"
​
        execute_values(cursor, insert_query, data_tuples)
        conn.commit()
        print(f"Inserted {len(data_tuples)} new rows into {table_name}.")
​
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data into {table_name}: {e}")
​
    finally:
        cursor.close()
        conn.close()  
        
​new_rows = check_for_new_data(dataframes["bids"],"procurement_data.csv", "bid_name")
print(new_rows)
push_updates_to_postgres(new_rows, "bids", db_params)