import requests
import csv
import time

# Function to load the API token from file
def load_api_token():
    with open("01_ETL/03_opentransportdata.swiss/token_ckan_opentransportdata.txt", "r") as file:
        return file.read().strip()

# API token and base URL
api_token = load_api_token()
base_url = "https://api.opentransportdata.swiss/ckan-api/"

# request-header
headers = {
    "Authorization": f"Bearer {api_token}",
    "User-Agent": "bruno",
    "Accept-Encoding": "zip, br, deflate"
}

# Step 1: Retrieve the list of datasets
response = requests.get(base_url + "package_list", headers=headers)

if response.status_code == 200:
    dataset_list = response.json()["result"]  # Get all datasets
else:
    print("Error retrieving datasets:", response.text)
    exit()

print(f"{len(dataset_list)} datasets found")

# List for storing all metadata fields
all_keys = set()
dataset_data = []
batch_size = 30  # Number of requests per batch

# Step 2: Retrieve metadata with rate limit control
for i in range(0, len(dataset_list), batch_size):
    batch = dataset_list[i:i + batch_size]
    
    for dataset_name in batch:
        dataset_response = requests.get(base_url + f"package_show?id={dataset_name}", headers=headers)

        if dataset_response.status_code == 200:
            dataset = dataset_response.json()["result"]
            dataset_data.append(dataset)  # Save the dataset for later
            all_keys.update(dataset.keys())  # Collect all available metadata fields
        else:
            print(f"Error retrieving {dataset_name}: {dataset_response.text}")

    print(f"{i + batch_size} out of {len(dataset_list)} processed... Waiting for 60 seconds")
    time.sleep(60)  # Wait for 1 minute to avoid hitting the API limit

# Step 3: Save to CSV
csv_filename = "01_ETL/03_opentransportdata.swiss/dataset_metadata.csv"

with open(csv_filename, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    
    # Write the column headers dynamically based on all the found metadata fields
    all_keys = sorted(all_keys)  # Sort for better readability
    writer.writerow(all_keys)
    
    # Write the values for each dataset
    for dataset in dataset_data:
        writer.writerow([dataset.get(key, "N/A") for key in all_keys])

print(f"CSV file '{csv_filename}' has been successfully created!")
