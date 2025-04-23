
import requests
import json


search_url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

headers = {
    "Content-Type": "application/json"
}


payload = {

      "limit": 100,
      "page": 1,

      "filters": {
          "award_type_codes":["02","03","04","05"],
          "agencies":[{"type":"awarding","tier":"toptier","name":"Department of Housing and Urban Development"}],
          "place_of_performance_locations":[{"country":"USA","state":"PR"}]
          
      },
      "fields": [
          "Award ID",
          "Recipient Name",
          "Start Date",
          "End Date",
          "Award Amount",
          "Awarding Agency",
          "Awarding Sub Agency",
          "Contract Award Type",
          "Award Type",
          "Funding Agency",
          "Funding Sub Agency"
      ]
  }

response = requests.post(search_url, headers=headers, data=json.dumps(payload))

if response.status_code != 200:
    print(f"Error fetching transactions: {response.status_code}")
    print(response.text)
    exit()

transactions = response.json().get("results", [])
transaction_ids = []


for award in transactions:

    transaction_ids.append(award.get('generated_internal_id'))
    
award_url = "https://api.usaspending.gov/api/v2/awards/"+transaction_ids[0]

payload = {

      "limit": 100,
      "page": 1,
      
      "filters": {
          "award_type_codes":["02","03","04","05"]
          
      },

   
      "fields": [
    'Award ID',
    'Recipient Name',
    'Recipient DUNS Number',
    'recipient_id',
    'Awarding Agency',
    'Awarding Agency Code',
    'Awarding Sub Agency',
    'Awarding Sub Agency Code',
    'Funding Agency',
    'Funding Agency Code',
    'Funding Sub Agency',
    'Funding Sub Agency Code',
    'Place of Performance City Code',
    'Place of Performance State Code',
    'Place of Performance Country Code',
    'Place of Performance Zip5',
    'Description',
    'Last Modified Date',
    'Base Obligation Date',
    'prime_award_recipient_id',
    'generated_internal_id',
    'def_codes',
    'COVID-19 Obligations',
    'COVID-19 Outlays',
    'Infrastructure Obligations',
    'Infrastructure Outlays',
    'Start Date',
    'End Date',
    'Award Amount',
    'Total Outlays',
    'Award Type',
    'SAI Number', 
    'CFDA Number'
]
  }

response = requests.post(search_url, headers=headers, data=json.dumps(payload))
if response.status_code != 200:
    print(f"Error fetching transactions: {response.status_code}")
    print(response.text)
    exit()

transactions = response.json().get("results", [])

print(transactions[0])


