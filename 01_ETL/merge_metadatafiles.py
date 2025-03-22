import pandas as pd
from typing import List
import os
from error_logger import log_error

def merge_csv_files(input_files: List[str], output_file: str) -> None:
    """
    Merge multiple CSV files into one, keeping all columns from all files.

    Parameters:
    -----------
    input_files : List[str]
        List of paths to input CSV files.
    output_file : str
        Path to the output CSV file where the merged data will be saved.

    Returns:
    --------
    None
    """
    try:
        dataframes = [pd.read_csv(file) for file in input_files]
        merged_df = pd.concat(dataframes, ignore_index=True, sort=False)
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        merged_df.to_csv(output_file, index=False)
        print(f"Merging completed. '{output_file}' saved successfully!")
    except Exception as e:
        log_error(f"Failed to merge and save CSV files to '{output_file}'", level="error", exception=e)

def merge_all_metadata(folders: List[str], output_dir: str) -> None:
    """
    Merge dataset, distribution, and contact metadata CSV files from given folders.

    Parameters:
    -----------
    folders : List[str]
        List of folder paths that contain the metadata files.
    output_dir : str
        Path to the directory where merged CSV files should be saved.

    Returns:
    --------
    None
    """
    suffixes = {
        'dataset': '_dataset_metadata.csv',
        'distribution': '_distribution_metadata.csv',
        'contact_point': '_contact_point_metadata.csv'
    }

    try:
        for key, suffix in suffixes.items():
            input_paths = []
            for folder in folders:
                for file in os.listdir(folder):
                    if file.endswith(suffix):
                        input_paths.append(os.path.join(folder, file))

            if input_paths:
                output_file = os.path.join(output_dir, f"merged_{key}_metadata.csv")
                merge_csv_files(input_paths, output_file)
            else:
                print(f"No files found for '{key}' with suffix '{suffix}' in folders: {folders}")

        log_error("All metadata files successfully merged.", level="info")

    except Exception as e:
        log_error("An error occurred while merging metadata files.", level="error", exception=e)