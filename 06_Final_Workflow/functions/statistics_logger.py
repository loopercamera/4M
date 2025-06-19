import csv
import os
from datetime import datetime

# Define CSV file name
CSV_FILE = "06_Final_Workflow\data\portal_statistics.csv"

# Clear the log file before each run
if os.path.exists(CSV_FILE):
    with open(CSV_FILE, "w") as file:
        file.truncate(0)  # Clears the file content


# Check if file exists; if not, create it with headers
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Timestamp", "Portal Name", "Step Name", "Successful", "Failed"])  # Header row

# Dictionary to store statistics temporarily
portal_stats = {}

def log_portal_result(portal_name, step_name, success=True):
    """
    Log results for a given portal and step into the dictionary.

    Args:
        portal_name (str): The name of the portal.
        step_name (str): The step at which success or failure occurred.
        success (bool | int): True if the process was successful, False if it failed, or an integer for dataset count.

    Returns:
        None
    """
    key = (portal_name, step_name)

    if key not in portal_stats:
        portal_stats[key] = {"success": 0, "fail": 0}

    if isinstance(success, bool):  # Standard success tracking
        if success:
            portal_stats[key]["success"] += 1
        else:
            portal_stats[key]["fail"] += 1
    elif isinstance(success, int):  # Special case for dataset count
        portal_stats[key]["success"] = success

def count_datasets_in_csv(csv_path, portal_name):
    """
    Count the number of datasets in a given CSV file and log it.

    Args:
        csv_path (str): Path to the CSV file containing dataset names.
        portal_name (str): Name of the portal being processed.

    Returns:
        int: Number of datasets counted.
    """
    if not os.path.exists(csv_path):
        log_portal_result(portal_name, "Dataset Count", success=0)
        return 0  # File doesn't exist

    try:
        with open(csv_path, mode="r", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader, None)  # Skip header row
            dataset_count = sum(1 for _ in reader)  # Count remaining rows

        log_portal_result(portal_name, "Dataset Count", success=dataset_count)
        return dataset_count

    except Exception as e:
        log_portal_result(portal_name, "Dataset Count", success=False)
        return 0

def save_statistics():
    """
    Save the collected statistics to a CSV file.

    Returns:
        None
    """
    with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        
        for (portal, step), stats in portal_stats.items():
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow([timestamp, portal, step, stats["success"], stats["fail"]])

    # Clear dictionary after saving to prevent duplicate entries
    portal_stats.clear()
