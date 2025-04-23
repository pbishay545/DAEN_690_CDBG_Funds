import requests
import json
import time
import pandas as pd


search_url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
award_url_base = "https://api.usaspending.gov/api/v2/awards/"

headers = {
    "Content-Type": "application/json"
}


all_transactions = []
page = 1
limit = 100  

while True:
    print(f"Fetching page {page}...")
    
    search_payload = {
        "limit": limit,
        "page": page,
        "filters": {
            "award_type_codes": ["02", "03", "04", "05"],
            "agencies": [{"type": "awarding", "tier": "toptier", "name": "Department of Housing and Urban Development"}],
            "place_of_performance_locations": [{"country": "USA", "state": "PR"}]
        },
        "fields": ["Award ID", "generated_internal_id"]
    }

    response = requests.post(search_url, headers=headers, data=json.dumps(search_payload))
    response.raise_for_status()
    results = response.json().get("results", [])

    if not results:
        print("No more results.")
        break

    all_transactions.extend(results)
    page += 1
    time.sleep(0.2)  

print(f"Collected {len(all_transactions)} awards.")


transaction_ids = [t["generated_internal_id"] for t in all_transactions if t.get("generated_internal_id")]

award_details = []

for i, transaction_id in enumerate(transaction_ids):
    url = award_url_base + transaction_id
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        award_details.append(data)
    except Exception as e:
        print(f"Error fetching award ID {transaction_id}: {e}")
    time.sleep(0.2)  # Be kind to the API

print(f"Fetched detailed data for {len(award_details)} awards.")

df_awards = pd.json_normalize(award_details)
print(df_awards.head())


df_awards.to_csv("hud_awards_puerto_rico.csv", index=False)