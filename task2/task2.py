import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import time
 
url = 'https://developer.uspto.gov/ptab-api/proceedings'
 
params = {
    'recordTotalQuantity': '10',
    'recordStartNumber': '0'
}
 
headers = {
    'accept': 'application/json'
}
 
total_records_to_fetch = 100
records_per_request = 10
current_start_number = 0
data_list = []
 
parquet_file = 'ptab_proceedings_key_columns.parquet'
 
# Define the schema for consistency
key_columns = [
    'proceedingNumber',
    'proceedingFilingDate',
    'proceedingStatusCategory',
    'proceedingTypeCategory',
    'respondentPartyName',
    'appellantPartyName'
]
 
schema = pa.schema([
    ('proceedingNumber', pa.string()),
    ('proceedingFilingDate', pa.string()),
    ('proceedingStatusCategory', pa.string()),
    ('proceedingTypeCategory', pa.string()),
    ('respondentPartyName', pa.string()),
    ('appellantPartyName', pa.string())
])
 
try:
    while current_start_number < total_records_to_fetch:
        params['recordStartNumber'] = str(current_start_number)
 
        response = requests.get(url, params=params, headers=headers)
 
        if response.status_code == 200:
            data = response.json()
            print(f"Fetched {len(data['results'])} records starting from index {current_start_number}:")
            data_list.extend(data['results'])
 
            # Convert data to DataFrame
            df = pd.DataFrame(data['results'])
            # Print the column names to identify the correct key columns
            print("Column names in the DataFrame:", df.columns.tolist())
            # Ensure all key columns are present
            for col in key_columns:
                if col not in df.columns:
                    df[col] = None
            # Select the key columns in the correct order
            df_key_columns = df[key_columns]
 
            # Append to Parquet file
            table = pa.Table.from_pandas(df_key_columns, schema=schema)
            if not os.path.exists(parquet_file):
                pq.write_table(table, parquet_file)
            else:
                existing_table = pq.read_table(parquet_file)
                combined_table = pa.concat_tables([existing_table, table])
                pq.write_table(combined_table, parquet_file)
 
            print(f"Appended records starting from index {current_start_number} to {parquet_file}")
 
            current_start_number += records_per_request
 
            # Wait for 30 seconds before fetching the next set of records
            time.sleep(30)
        else:
            print(f"Request failed with status code {response.status_code}")
            print(response.text)
            break
 
except requests.exceptions.RequestException as e:
    print(f"Error with API request: {e}")
 
print(f"Data saved to {parquet_file}")
