"""
Module for post-processing CSV metadata files extracted from DCAT-AP-CH XML metadata.
Includes:
- Sorting language fields
- Formatting ISO date strings
- Cleaning values like 'N/A' and empty list strings

Use `postprocess_all()` to apply all steps.
"""

import pandas as pd
import ast
from datetime import datetime
import os
import xml.etree.ElementTree as ET
from error_logger import log_error  # Import log_error from error_logger.py


values_to_remove = {"N/A", "[N/A]","['N/A']"}

def sort_languages_in_column(df, column):
    """Sorts and normalizes language entries in a column if the values are lists."""
    if column in df.columns:
        df[column] = df[column].apply(lambda x: sorted(eval(x)) if isinstance(x, str) else x)
    return df

def transform_date(date_str):
    """Converts ISO-formatted date strings to a standardized format. Returns 'N/A' for missing or invalid values."""
    try:
        if pd.isna(date_str) or str(date_str).strip() == "":
            return "N/A"
        return datetime.fromisoformat(date_str).strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return date_str

def clean_value(value):
    """Cleans a single value by removing placeholders such as 'N/A' or '[N/A]'. Also cleans list values."""
    if isinstance(value, list):
        return [item for item in value if str(item).strip() not in values_to_remove] or ""

    if pd.isna(value) or str(value).strip() in values_to_remove:
        return ""
    try:
        parsed_value = ast.literal_eval(value)
        if isinstance(parsed_value, list):
            parsed_value = [item for item in parsed_value if str(item).strip() not in values_to_remove]
            return parsed_value if parsed_value else ""
    except (ValueError, SyntaxError):
        pass
    return str(value).strip()

def process_dataset_metadata(path):
    """Cleans and transforms the dataset metadata CSV file at the given path."""
    df = pd.read_csv(path, dtype=str)
    df = sort_languages_in_column(df, "dataset_language")
    for col in ["dataset_issued", "dataset_modified", "dataset_temporal_startDate", "dataset_temporal_endDate"]:
        if col in df.columns:
            df[col] = df[col].apply(transform_date)
    for col in df.columns:
        df[col] = df[col].map(clean_value)
    df.to_csv(path, index=False)

def process_distribution_metadata(path):
    """Cleans and transforms the distribution metadata CSV file at the given path."""
    df = pd.read_csv(path, dtype=str)
    df = sort_languages_in_column(df, "distribution_language")
    for col in ["distribution_issued_date", "distribution_modified_date"]:
        if col in df.columns:
            df[col] = df[col].apply(transform_date)
    for col in df.columns:
        df[col] = df[col].map(clean_value)
    df.to_csv(path, index=False)

def process_contact_metadata(path):
    """Cleans and transforms the contact metadata CSV file at the given path."""
    df = pd.read_csv(path, dtype=str)
    for col in df.columns:
        df[col] = df[col].map(clean_value)
    df.to_csv(path, index=False)

def transform_opendata_swiss(folder_path):
    """Runs post-processing on all standard metadata CSV files in the current directory."""
    log_error(f"Transformation of opendata.swiss metadata datasets started", "info")
    try:
        process_dataset_metadata(os.path.join(folder_path, "opendata_dataset_metadata.csv"))
        process_distribution_metadata(os.path.join(folder_path, "opendata_distribution_metadata.csv"))
        process_contact_metadata(os.path.join(folder_path, "opendata_contact_point_metadata.csv"))
        log_error(f"Transformation of opendata.swiss metadata datasets finished successfully", "info")
    except ET.ParseError as e:
        log_error(f"Transformation of opendata.swiss metadata datasets failed", "error", e)

if __name__ == "__main__":
    import os
    log_error(f"Transformation of opendata.swiss metadata datasets started", "info")
    try:
        transform_opendata_swiss(".")
        log_error(f"Transformation of opendata.swiss metadata datasets finished successfully", "info")
    except ET.ParseError as e:
        log_error(f"Transformation of opendata.swiss metadata datasets failed", "error", e)

