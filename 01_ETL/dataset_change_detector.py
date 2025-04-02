"""
This script calculates SHA-256 hashes for files in a specified folder and compares
current files with a previously stored list to identify 'new', 'changed', 'found', or 'removed' datasets.
It outputs:
- A CSV file of the latest import
- A comparison CSV result
- A CSV metadata file for removed/changed entries
- An updated previous import file

The file format (extension), working directory path, and metadata output file can be passed as command-line arguments,
or the script can be imported and called via the function `compare_dataset_hashes()`.
"""
from statistics_logger import log_portal_result, save_statistics
import os
import hashlib
import pandas as pd
import argparse

def file_hash(file_path):
    """
    Compute the SHA-256 hash of a file.

    Args:
        file_path (str): Path to the file.

    Returns:
        str or None: The SHA-256 hash string or None if file not found.
    """
    try:
        with open(file_path, 'rb') as f:
            return hashlib.sha256(f.read()).hexdigest()
    except FileNotFoundError:
        return None

def get_files_by_extension(directory, extension="xml"):
    """
    List files in a directory with a specific extension.

    Args:
        directory (str): Directory to search in.
        extension (str): File extension to filter by (default: "xml").

    Returns:
        list of str: List of matching filenames.
    """
    return [f for f in os.listdir(directory) if f.lower().endswith(f".{extension.lower()}")]


def compare_dataset_hashes(base_dir, remove_order_file, extension="xml", portal_name="opendata.swiss"):

    folder_path = os.path.join(base_dir, "saved_metadata_xml")
    latest_import = os.path.join(base_dir, "latest_import.csv")
    previous_import = os.path.join(base_dir, "previous_import.csv")
    comparison_file = os.path.join(base_dir, "comparison_result.csv")

    # List all relevant files and compute their hashes
    dataset_names = []
    dataset_hashes = []

    for filename in get_files_by_extension(folder_path, extension=extension):
        dataset_names.append(filename[:-(len(extension) + 1)])
        file_path = os.path.join(folder_path, filename)
        dataset_hashes.append(file_hash(file_path))

    latest_df = pd.DataFrame({"Dataset_Name": dataset_names, "dataset_hash": dataset_hashes})
    latest_df.to_csv(latest_import, index=False)

    if os.path.exists(previous_import):
        previous_df = pd.read_csv(previous_import)
        latest_df["Dataset_Name"] = latest_df["Dataset_Name"].astype(str)
        previous_df["Dataset_Name"] = previous_df["Dataset_Name"].astype(str)

        if not latest_df.empty:
            comparison_df = pd.merge(latest_df, previous_df, on="Dataset_Name", how="left", suffixes=('_latest', '_previous'))

            def get_status(row):
                if pd.isna(row["dataset_hash_previous"]):
                    return "new"
                elif row["dataset_hash_latest"] == row["dataset_hash_previous"]:
                    return "found"
                else:
                    return "changed"

            comparison_df["status"] = comparison_df.apply(get_status, axis=1)
            comparison_df.rename(columns={"dataset_hash_latest": "dataset_hash"}, inplace=True)
            comparison_df = comparison_df.drop(columns=["dataset_hash_previous"])
        else:
            comparison_df = pd.DataFrame(columns=["Dataset_Name", "dataset_hash", "status"])

        removed_df = previous_df[~previous_df["Dataset_Name"].isin(latest_df["Dataset_Name"])].copy()
        removed_df["status"] = "removed"
        final_df = pd.concat([comparison_df, removed_df], ignore_index=True)
        final_df.to_csv(comparison_file, index=False)
    else:
        latest_df["status"] = "new"
        final_df = latest_df

    final_df.to_csv(comparison_file, index=False)

    # Logging summary statistics
    status_counts = final_df["status"].value_counts().to_dict()
    for status, count in status_counts.items():
        log_portal_result(portal_name, f"Hash Status: {status}", success=count)
    save_statistics()
    print("Hashes calculated and saved to comparison_result.csv")

    # use in-memory final_df instead of reloading CSV
    df = final_df.copy()
    files_to_remove = df[df["status"] == "found"]["Dataset_Name"].tolist()

    for file_name in files_to_remove:
        file_path = os.path.join(folder_path, file_name + f".{extension}")
        if os.path.exists(file_path):
            os.remove(file_path)
        else:
            print(f"File not found: {file_name}.{extension}")

    print("Cleanup complete.")

    remove_order_df = df[df["status"].isin(["changed", "removed"])]
    remove_order_df.to_csv(remove_order_file, index=False)
    print(f"Remove order metadata saved to {remove_order_file}")

    valid_statuses = ["found", "changed", "new"]
    filtered_df = df[df["status"].isin(valid_statuses)][["Dataset_Name", "dataset_hash"]]
    filtered_df.to_csv(previous_import, index=False)
    print("previous_import.csv has been updated with relevant dataset entries.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare file hashes and detect dataset changes.")
    parser.add_argument("--ext", type=str, default="xml", help="File extension to include (default: xml)")
    parser.add_argument("--dir", type=str, required=True, help="Path to the folder containing data files")
    parser.add_argument("--removeorder", type=str, default="removeorder_metadata.csv", help="Path to the remove order metadata CSV output")
    parser.add_argument("--portal", type=str, default="opendata.swiss", help="Name of the portal for logging")
    args = parser.parse_args()

    compare_dataset_hashes(args.dir, args.removeorder, args.ext, args.portal)
