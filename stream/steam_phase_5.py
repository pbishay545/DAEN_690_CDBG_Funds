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
    }

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def download_pdf(base_url, pdf_filename):



    response = requests.get(base_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract all links
        links = [a["href"] for a in soup.find_all("a", href=True)]

        # Filter for links that extend from base_url
        child_links = [urljoin(base_url, link) for link in links if link.startswith(base_url)]

        # Get the second link from the filtered child links
        if len(child_links) > 1:  # Make sure there is a second link
            second_link = child_links[1]
            print(f"Second PDF link: {second_link}")

            # Download the PDF from the second link
            content = requests.get(second_link)

            if content.status_code == 200:

    


                # Write the content to a file in the same directory
                with open(pdf_filename, 'wb') as f:
                    f.write(content.content)
                print(f"PDF downloaded as {pdf_filename}")
            else:
                print(f"Failed to download PDF. Status code: {content.status_code}")
        else:
            print("Second link not found.")
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")

import requests
import calendar
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


# Check if a date is valid
def is_valid_date(year, month, day):
    """Check if the given day exists in the month/year."""
    return 1 <= int(day) <= calendar.monthrange(int(year), int(month))[1]

def run_urls():
    QUARTER_MONTHS = {
    "1st": ["01", "02", "03"],  
    "2nd": ["04", "05", "06"],
    "3rd": ["07", "08", "09"],  
    "4th": ["10", "11", "12"]   
    }
    variations = {
        "quarter": ["1st", "2nd", "3rd", "4th"],
        "year": [str(x) for x in range(2010, 2025)],
        "day": [f"{i:02d}" for i in range(1, 32)]
    }

    counter = 0
    for quarter in variations["quarter"]:
        for year in variations["year"]:
            for month in QUARTER_MONTHS[quarter]:
                for day in variations["day"]:
                    if is_valid_date(int(year), int(month), int(day)):  
                        counter += 1
                        if counter %100 ==0:
                            print(f"run: {counter}")


                        url = f"https://recuperacion.pr.gov/en/download/{quarter}-qtr-{year}-dr-p-17-pr-72-him1-qprdownload-submitted-await-for-review-{month}{day}{year}/"

                        response = requests.get(url)
                        if response.status_code == 200:
                            print("made request!! {response}")
                            download_pdf(url, f"prdoh_pdfs/{quarter}-{month}-{day}-{year}" )

run_urls()

from pdf2image import convert_from_path
import easyocr
import numpy as np


poppler_path = "/scratch/pbishay/poppler-24.08.0/Library/bin"


def create_parsed_data(doc_path):
    poppler_path = "/scratch/pbishay/poppler-24.08.0/Library/bin"
    reader = easyocr.Reader(['en'])
    
    images = convert_from_path(doc_path, poppler_path=poppler_path)
    
    parsed_text = {}
    
    for page_num, img in enumerate(images, 1): 
    
        img_np = np.array(img)
    
    
        result = reader.readtext(img_np)
    
    
        page_text_lines = [detection[1] for detection in result]  
        
    
        parsed_text[page_num] = page_text_lines
    return parsed_text

def find_substring_key(dictionary, target_string):
    for key in dictionary:
        if key in target_string:
            return key  
    return None  

def extract_point(content, key, extract_dict, current_page_num, pages, fields):
    current_page = pages[current_page_num]
    
    # Handle type-specific extractions
    if extract_dict["type"] == "same_line":
        return content.split(": ")[-1]
        
    elif extract_dict["type"] == 1 or extract_dict["type"] == 2:
        # Fixed index logic: handle cases where the key might be directly followed by the value
        current_index = current_page.index(content)
        if current_index + 1 < len(current_page):
            return current_page[current_index + 2] 
        
    elif extract_dict["type"] == "next":
        current_index = current_page.index(content)
        if current_index + 1 < len(current_page):
            return current_page[current_index + 1]
        
    elif extract_dict["type"] == "next_two":
        current_index = current_page.index(content)
        values = []
        if current_index + 1 < len(current_page):
            values.append(current_page[current_index + 1])
        if current_index + 2 < len(current_page):
            values.append(current_page[current_index + 2])
        return values
        
    elif extract_dict["type"] == "continue_until_new_var":
        full_text = ""
        full_text_length = len(current_page)

        current_index = current_page.index(content)
        
        while current_index < full_text_length:
 
            current_text = current_page[current_index + 1] if current_index + 1 < len(current_page) else ""
            if find_substring_key(fields, current_text):
                break
            full_text += current_text
            current_index += 1
        return full_text

    elif extract_dict["type"] == "EOF_condition":
        full_text = ""
        full_text_length = len(current_page)
        current_index = current_page.index(content)
        i = current_index 
        # Start collecting content after the found key
        
        while True:     
            current_text = current_page[i]

            if current_text == extract_dict["EOF"] or find_substring_key(fields, current_text):
                return full_text, current_page_num +1
                break
                
            full_text += current_text + " "
            

            if i + 1 == full_text_length:
                i = 0
       
                if current_page_num + 1 < len(pages):
                    current_page = pages[current_page_num + 1]
                    current_page_num += 1
         
            i = i+1
        return full_text, current_page_num +1

def extract_data(pages, start_page):
    EOF_condition = "Accomplishments Performance Measures"
    fields = {
        "Grantee Activity Number": {"type": "same_line", "value":""},
        "Activity Title": {"type": "same_line", "value":""},
        "Activity Type": {"type": 1, "value":""},
        "Activity Status": {"type": 2, "value":""},
        "Project Number": {"type": 1, "value":""},
        "Project Title": {"type": 2, "value":""},
        "Projected Start Date": {"type": 1, "value":""},
        "Projected End Date": {"type": 2, "value":""},
        "Benefit Type": {"type": 1, "value":""},
        "National Objective": {"type": 1, "value":""},
        "Responsible Organization": {"type": 2, "value":""},
        "Program Income Account": {"type": "next", "value":""},
        "Total Projected Budget from All Sources": {"type": "next_two", "value":""},
        "Total Budget": {"type": "next_two", "value":""},
        "Total Obligated": {"type": "next_two", "value":""},
        "Total Funds Drawdown": {"type": "next_two", "value":""},
        "Program Funds Drawdown": {"type": "next_two", "value":""},
        "Program Income Drawdown": {"type": "next_two", "value":""},
        "Program Income Received": {"type": "next_two", "value":""},
        "Total Funds Expended": {"type": "next_two", "value":""},
        "Most Impacted and Distressed Expended": {"type": "next_two", "value":""},
        "Activity Description" : {"type": "continue_until_new_var", "value":""},
        "Actlvlty Descrlpton:" : {"type": "continue_until_new_var", "value":""}, 
        "Location Description" : {"type": "continue_until_new_var", "value":""},
        "Locatlon DescrIptlon" : {"type": "continue_until_new_var", "value":""},
        "Activity Progress Narrative" : {"type": "EOF_condition", "value":"", "EOF": EOF_condition}
    }
    current_page_num = start_page
    current_page = pages[current_page_num] 
    while True:
        if current_page_num >= len(pages):                    
            return fields, current_page_num
        for content in current_page:
            if content == current_page[-1]:
                current_page_num += 1
                if current_page_num >= len(pages):
                        
                    break
                    return fields, current_page_num
                else:
                    current_page = pages[current_page_num]
                    continue

            key_value = find_substring_key(fields, content)
            if key_value:
                if "EOF" in fields[key_value]:
                    value, last_page = extract_point(content, key_value, fields[key_value], current_page_num, pages, fields)
                    fields[key_value]["value"] = value
                    return fields, last_page
                else:
                    fields[key_value]["value"] = extract_point(content, key_value, fields[key_value], current_page_num, pages, fields)
                    
                    

def create_dataframe(parsed_text, file_name, current_page = 7)                  
    last_page = len(parsed_text)
    all_data = []
    while True:
        parsed_data, current_page = extract_data(parsed_text, current_page)
        all_data.append(parsed_data)
        if current_page +1 > last_page:
            break


    flattened_data = []
    for entry in all_data:
        flattened_entry = {key: val['value'] for key, val in entry.items()}
        flattened_data.append(flattened_entry)

    df = pd.DataFrame(flattened_data)

    print(df)
    df.to_csv('prdoh_extracted/output.csv', index=False)

    import os 
paths = os.listdir('prdoh_pdfs')
parsed_data = create_parsed_data("prdoh_pdfs/"+ paths[0])
print(parsed_data)
