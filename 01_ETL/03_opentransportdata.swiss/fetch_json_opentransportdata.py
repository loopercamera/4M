import os
import requests
import json
import time

# Function to load the API token from the file
def load_api_token():
    with open("01_ETL/03_opentransportdata.swiss/token_ckan_opentransportdata.txt", "r") as file:
        return file.read().strip()

# API token and base URL
api_token = load_api_token()
base_url = "https://api.opentransportdata.swiss/ckan-api/"

# Request headers
headers = {
    "Authorization": f"Bearer {api_token}",
    "User-Agent": "bruno",
    "Accept-Encoding": "zip, br, deflate"
}

# Step 1: Retrieve the list of datasets
response = requests.get(base_url + "package_list", headers=headers)

if response.status_code == 200:
    dataset_list = response.json()["result"] 
else:
    print("Error retrieving datasets:", response.text)
    exit()

print(f"{len(dataset_list)} datasets found")

# Create directory for JSON files
output_folder = "01_ETL/03_opentransportdata.swiss/json_files"
os.makedirs(output_folder, exist_ok=True)

# Step 2: Retrieve metadata and save as JSON files
batch_size = 45  # Number of datasets per request
for i in range(0, len(dataset_list), batch_size):
    batch = dataset_list[i:i + batch_size]
    
    for dataset_name in batch:
        dataset_response = requests.get(base_url + f"package_show?id={dataset_name}", headers=headers)

        if dataset_response.status_code == 200:
            dataset = dataset_response.json()["result"]
            
            # Check if the attribute "type" has the value "dataset"
            if dataset.get("type") == "dataset":
                # Save the response as a JSON file in the subfolder
                json_filename = os.path.join(output_folder, f"{dataset_name}.json")
                with open(json_filename, "w", encoding="utf-8") as json_file:
                    json.dump(dataset, json_file, ensure_ascii=False, indent=4)
                
                print(f"The file {json_filename} was successfully saved.")
            else:
                print(f"Dataset {dataset_name} was skipped because the type is not 'dataset'.")
        else:
            print(f"Error retrieving {dataset_name}: {dataset_response.text}")
    
    if i + batch_size < len(dataset_list):
        print(f"{i + batch_size} out of {len(dataset_list)} processed... Waiting for 60 seconds")
        time.sleep(60)  # Wait to avoid hitting the API rate limit
    
    if i + batch_size >= len(dataset_list):
        print(f"All datasets processed: {len(dataset_list)} out of {len(dataset_list)} datasets")
