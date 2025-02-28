import requests
import json
import csv

def opendata_swiss_online():
    url = "https://ckan.opendata.swiss/api/3/action/status_show"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("success", False)
    except requests.exceptions.RequestException:
        return False

def save_datasets(json_file="01_ETL/01_opendata.swiss/opendata_swiss_datasets.json", csv_file="01_ETL/01_opendata.swiss/opendata_swiss_datasets.csv"):
    if not opendata_swiss_online():
        print("API is offline :(")
        return

    url = "https://ckan.opendata.swiss/api/3/action/package_list"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()

        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        print(f"JSON data saved to {json_file}")

        # Extract the list of dataset names
        datasets = data.get("result", [])

        # Write to a CSV file
        with open(csv_file, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["Dataset Name"])  # Write the header
            for dataset in datasets:
                writer.writerow([dataset])

        print(f"CSV file '{csv_file}' has been created successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")

if __name__ == "__main__":
    save_datasets()
