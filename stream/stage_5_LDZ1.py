

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

                        url = f"https://recuperacion.pr.gov/en/download/{quarter}-qtr-{year}-eq-p-21-pr-72-ldz1-qprdownload-reviewed-and-approved-for-review-{month}{day}{year}/"

                        response = requests.get(url)
                        if response.status_code == 200:
                            print("made request!! {response}")
                            download_pdf(url, f"prdoh_pdfs/{quarter}-{month}-{day}-{year}" )

run_urls()

import pandas as pd
import os

def find_substring_key(dictionary, target_string):
    for key in dictionary:
        if key in target_string:
            return key
    return None

def extract_point(content, key, extract_dict, current_page_num, pages, fields):
    current_page = pages[current_page_num]
    
    if extract_dict["type"] == "same_line":
        return content.split(": ")[-1]
    
    elif extract_dict["type"] in [1, 2]:
        try:
            current_index = current_page.index(content)
            return current_page[current_index + 2]  # fixed to +2
        except (ValueError, IndexError):
            return ""
    
    elif extract_dict["type"] == "next":
        try:
            current_index = current_page.index(content)
            return current_page[current_index + 1]
        except (ValueError, IndexError):
            return ""
    
    elif extract_dict["type"] == "next_two":
        try:
            current_index = current_page.index(content)
            values = []
            if current_index + 1 < len(current_page):
                values.append(current_page[current_index + 1])
            if current_index + 2 < len(current_page):
                values.append(current_page[current_index + 2])
            return " ".join(values)
        except (ValueError, IndexError):
            return ""
    
    elif extract_dict["type"] == "continue_until_new_var":
        try:
            current_index = current_page.index(content)
            full_text = ""
            while current_page_num < len(pages):
                while current_index + 1 < len(current_page):
                    current_index += 1
                    current_text = current_page[current_index]
                    if find_substring_key(fields, current_text):
                        return full_text.strip()
                    full_text += " " + current_text
                # move to next page
                current_page_num += 1
                if current_page_num < len(pages):
                    current_page = pages[current_page_num]
                    current_index = -1  # reset
            return full_text.strip()
        except (ValueError, IndexError):
            return ""
    
    elif extract_dict["type"] == "EOF_condition":
        try:
            current_index = current_page.index(content)
            full_text = ""
            while current_page_num < len(pages):
                while current_index + 1 < len(current_page):
                    current_index += 1
                    current_text = current_page[current_index]
                    if current_text == extract_dict["EOF"] or find_substring_key(fields, current_text):
                        return full_text.strip(), current_page_num + 1
                    full_text += " " + current_text
                # move to next page
                current_page_num += 1
                if current_page_num < len(pages):
                    current_page = pages[current_page_num]
                    current_index = -1  # reset
            return full_text.strip(), current_page_num
        except (ValueError, IndexError):
            return "", current_page_num
    return ""

def extract_data(pages, start_page):
    EOF_condition = "Accomplishments Performance Measures"
    
    fields = {
        "Grantee Activity Number": {"type": "same_line", "value": ""},
        "Activity Title": {"type": "same_line", "value": ""},
        "Activity Type": {"type": 1, "value": ""},
        "Activity Status": {"type": 2, "value": ""},
        "Project Number": {"type": 1, "value": ""},
        "Project Title": {"type": 2, "value": ""},
        "Projected Start Date": {"type": 1, "value": ""},
        "Projected End Date": {"type": 2, "value": ""},
        "Benefit Type": {"type": 1, "value": ""},
        "National Objective": {"type": 1, "value": ""},
        "Responsible Organization": {"type": 2, "value": ""},
        "Program Income Account": {"type": "next", "value": ""},
        "Total Projected Budget from All Sources": {"type": "next_two", "value": ""},
        "Total Budget": {"type": "next_two", "value": ""},
        "Total Obligated": {"type": "next_two", "value": ""},
        "Total Funds Drawdown": {"type": "next_two", "value": ""},
        "Program Funds Drawdown": {"type": "next_two", "value": ""},
        "Program Income Drawdown": {"type": "next_two", "value": ""},
        "Program Income Received": {"type": "next_two", "value": ""},
        "Total Funds Expended": {"type": "next_two", "value": ""},
        "Most Impacted and Distressed Expended": {"type": "next_two", "value": ""},
        "Activity Description": {"type": "continue_until_new_var", "value": ""},
        "Actlvlty Descrlpton:": {"type": "continue_until_new_var", "value": ""},
        "Location Description": {"type": "continue_until_new_var", "value": ""},
        "Locatlon DescrIptlon": {"type": "continue_until_new_var", "value": ""},
        "Activity Progress Narrative": {"type": "EOF_condition", "value": "", "EOF": EOF_condition}
    }
    
    current_page_num = start_page
    
    while current_page_num < len(pages):
        current_page = pages[current_page_num]
        
        for idx, content in enumerate(current_page):
            key_value = find_substring_key(fields, content)
            if key_value:
                if "EOF" in fields[key_value]:
                    value, last_page = extract_point(content, key_value, fields[key_value], current_page_num, pages, fields)
                    fields[key_value]["value"] = value
                    return fields, last_page
                else:
                    value = extract_point(content, key_value, fields[key_value], current_page_num, pages, fields)
                    fields[key_value]["value"] = value
        
        # After finishing current page
        current_page_num += 1
    
    return fields, current_page_num

def create_all_data(parsed_text):
    current_page = 7
    last_page = len(parsed_text)
    all_data = []
    
    while current_page < last_page:
        parsed_data, current_page = extract_data(parsed_text, current_page)
        all_data.append(parsed_data)
        if current_page >= last_page:
            break
    return all_data

def create_dataframe(all_data, file_name):
    flattened_data = [{key: val['value'] for key, val in entry.items()} for entry in all_data]
    df = pd.DataFrame(flattened_data)
    os.makedirs("results", exist_ok=True)
    output_path = os.path.join("results", f"{file_name}.csv")
    df.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")

def process_folder(base_path):
    array = os.listdir(base_path)
    for pdf in array:
        full_path = os.path.join(base_path, pdf)
        parsed_text = create_parsed_data(full_path)  # assuming you have this
        all_data = create_all_data(parsed_text)
        create_dataframe(all_data, pdf)

import os
base_path = "PATH"
array = os.listdir(base_path)

for pdf in array:
    full_path =os.path.join(base_path, pdf)
    all_data = create_all_data(full_path)
    create_dataframe(all_data, pdf)
    process_folder(base_path)
    