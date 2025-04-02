import pandas as pd
from typing import List
import os
from error_logger import log_error

# Column definitions for each metadata type
DATASET_COLUMNS = [
    "dataset_identifier",
    "origin",
    "xml_filename",
    "dataset_language",

    "dataset_title_DE",
    "dataset_keyword_DE",
    "dataset_description_DE",

    "dataset_title",
    "dataset_keyword_UNKNOWN",
    "dataset_description",

    "dataset_title_EN",
    "dataset_keyword_EN",
    "dataset_description_EN",

    "dataset_keyword_FR",
    "dataset_title_FR",
    "dataset_description_FR",

    "dataset_title_IT",
    "dataset_keyword_IT",
    "dataset_description_IT",

    "dataset_title_RM",
    "dataset_keyword_RM",
    "dataset_description_RM",

    "dataset_publisher_name",
    "dataset_publisher_URL",

    "dataset_theme",
    "dataset_issued",

]

DISTRIBUTION_COLUMNS = [
    "dataset_identifier",
    "distribution_format",
    "distribution_access_url",
    "origin",
    "xml_filename",
    "distribution_title_DE",
    "distribution_description_DE",
    "distribution_title_UNKNOWN",
    "distribution_description_UNKNOWN",
    "distribution_title_EN",
    "distribution_description_EN",
    "distribution_title_FR",
    "distribution_description_FR",
    "distribution_title_IT",
    "distribution_description_IT",
    "distribution_title_RM",
    "distribution_description_RM",
    "distribution_identifier",
    "distribution_media_type",
    "distribution_language",
    "distribution_download_url",
    "distribution_coverage",
    "distribution_temporal_resolution",
    "distribution_documentation",
    "distribution_id",
    "distribution_issued_date",
    "distribution_modified_date",
    "distribution_license",
    "distribution_rights",
    "distribution_byte_size"
]


CONTACT_POINT_COLUMNS = [
    "dataset_identifier", "contact_type", "contact_nodeID", "contact_email", "contact_name", "origin", "xml_filename"
]

# Template functions for merging

def merge_csv_files(input_files: List[str], output_file: str, required_columns: List[str]) -> None:
    """
    Merge multiple CSV files into one, keeping all required columns in a fixed order.
    """
    try:
        dataframes = []
        for file in input_files:
            df = pd.read_csv(file)
            for col in required_columns:
                if col not in df.columns:
                    df[col] = ""
            df = df[required_columns]
            dataframes.append(df)

        merged_df = pd.concat(dataframes, ignore_index=True, sort=False)

        for col in required_columns:
            if col not in merged_df.columns:
                merged_df[col] = ""

        merged_df = merged_df[required_columns]

        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        merged_df.to_csv(output_file, index=False)
        print(f"Merging completed. '{output_file}' saved successfully!")
    except Exception as e:
        log_error(f"Failed to merge and save CSV files to '{output_file}'", level="error", exception=e)


def merge_dataset_metadata(folders: List[str], output_dir: str) -> None:
    suffix = '_dataset_metadata.csv'
    input_paths = [
        os.path.join(folder, file)
        for folder in folders
        for file in os.listdir(folder)
        if file.endswith(suffix)
    ]

    if input_paths:
        output_file = os.path.join(output_dir, "merged_dataset_metadata.csv")
        merge_csv_files(input_paths, output_file, required_columns=DATASET_COLUMNS)
    else:
        print(f"No dataset metadata files found with suffix '{suffix}' in folders: {folders}")


def merge_distribution_metadata(folders: List[str], output_dir: str) -> None:
    suffix = '_distribution_metadata.csv'
    input_paths = [
        os.path.join(folder, file)
        for folder in folders
        for file in os.listdir(folder)
        if file.endswith(suffix)
    ]

    if input_paths:
        output_file = os.path.join(output_dir, "merged_distribution_metadata.csv")
        merge_csv_files(input_paths, output_file, required_columns=DISTRIBUTION_COLUMNS)
    else:
        print(f"No distribution metadata files found with suffix '{suffix}' in folders: {folders}")


def merge_contact_point_metadata(folders: List[str], output_dir: str) -> None:
    suffix = '_contact_point_metadata.csv'
    input_paths = [
        os.path.join(folder, file)
        for folder in folders
        for file in os.listdir(folder)
        if file.endswith(suffix)
    ]

    if input_paths:
        output_file = os.path.join(output_dir, "merged_contact_point_metadata.csv")
        merge_csv_files(input_paths, output_file, required_columns=CONTACT_POINT_COLUMNS)
    else:
        print(f"No contact point metadata files found with suffix '{suffix}' in folders: {folders}")


def merge_all_metadata(folders: List[str], output_dir):
    """Merge all metadata files from the specified folders into one output directory in the correct format."""
    log_error(f"Start merging of files", level="info")
    try:
        merge_dataset_metadata(folders, output_dir)
        log_error(f"Dataset files successful merged", level="info")
    except Exception as e:
        log_error(f"Failed to merge datasets Files" , level="error", exception=e)
    try:
        merge_distribution_metadata(folders, output_dir)
        log_error(f"Distribution files successful merged", level="info")
    except Exception as e:
        log_error(f"Failed to merge distribution Files" , level="error", exception=e)

    try:
        merge_contact_point_metadata(folders, output_dir)
        log_error(f"Contact Point files successful merged", level="info")
    except Exception as e:
        log_error(f"Failed to merge contact point Files" , level="error", exception=e)