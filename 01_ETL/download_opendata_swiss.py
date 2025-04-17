"""
Module: Download OpenData Swiss Datasets

This module interacts with the OpenData.swiss portal to fetch dataset metadata,
store dataset names into a CSV file, and download and process individual dataset
metadata in RDF/XML format. It performs the following tasks:

1. Fetches the list of dataset identifiers from the OpenData.swiss CKAN API.
2. Saves the dataset names into a local CSV file.
3. Downloads RDF/XML metadata for each dataset and applies the following processing:
   - Parses key fields (title, description, modification date).
   - Removes blank node identifiers and license elements for uniformity.
   - Sorts elements and attributes for consistent structure.
   - Prettifies and saves the final cleaned XML to disk.
4. Logs success or failure of each operation for monitoring and statistics.

Logging is handled via `error_logger` and `statistics_logger` modules.

Constants:
- PORTAL_NAME: Name of the portal being processed.
- MAX_DATASETS: Optional limit on the number of datasets to process.

Dependencies:
- requests
- csv
- os
- xml.etree.ElementTree
- xml.dom.minidom
- error_logger
- statistics_logger
"""

import requests
import csv
import os
import xml.etree.ElementTree as ET
from xml.dom import minidom
from error_logger import log_error
from statistics_logger import log_portal_result, save_statistics, count_datasets_in_csv

PORTAL_NAME = "opendata.swiss"
MAX_DATASETS = None  # Set to None to download all datasets

def fetch_datasets():
    """
    Fetches the list of dataset identifiers from the OpenData.swiss CKAN API.

    Returns:
        list: A list of dataset names/identifiers.
    """
    url = "https://ckan.opendata.swiss/api/3/action/package_list"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if "result" in data:
            dataset_names = data["result"]
            log_portal_result(PORTAL_NAME, "Fetch Dataset Names", success=True)
            return dataset_names
        else:
            log_error(f"Invalid response structure from {url}", "error")
            log_portal_result(PORTAL_NAME, "Fetch Dataset Names", success=False)
            return []
    except requests.exceptions.RequestException as e:
        log_error(f"Failed to fetch dataset names from {url}", "error", e)
        log_portal_result(PORTAL_NAME, "Fetch Dataset Names", success=False)
        return []

def save_datasets_to_csv(dataset_names, save_path):
    """
    Saves the list of dataset names to a CSV file.

    Args:
        dataset_names (list): List of dataset identifiers.
        save_path (str): Path where the CSV file will be saved.
    """
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    try:
        with open(save_path, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["Dataset_Name"])
            for dataset in dataset_names:
                writer.writerow([dataset])
        log_portal_result(PORTAL_NAME, "Save Dataset Names to CSV", success=True)
        log_error(f"All datasets have been successfully saved to {save_path}", "info")
    except Exception as e:
        log_error(f"Failed to save dataset names to {save_path}", "error", e)
        log_portal_result(PORTAL_NAME, "Save Dataset Names to CSV", success=False)

def gather_opendata_swiss_datasets():
    """
    Coordinates the process of fetching dataset names and saving them to a CSV file.
    Also logs statistics after the operation.
    """
    save_path = "01_ETL/01_opendata.swiss/opendata_swiss_datasets.csv"
    dataset_names = fetch_datasets()
    if dataset_names:
        save_datasets_to_csv(dataset_names, save_path)
    count_datasets_in_csv(save_path, PORTAL_NAME)
    log_error(f"Finished downloading and processing {len(dataset_names)} datasets.", "info")
    save_statistics()

def fetch_xml_metadata(identifier):
    """
    Fetches the RDF/XML metadata content for a given dataset identifier.

    Args:
        identifier (str): Dataset identifier.

    Returns:
        bytes or None: Raw XML content if successful, otherwise None.
    """
    url = f"https://ckan.opendata.swiss/dataset/{identifier}.xml"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        log_error(f"Failed to fetch XML metadata for {identifier}", "error", e)
        log_portal_result(PORTAL_NAME, "Fetch XML Metadata", success=False)
        return None

def parse_xml_metadata(xml_content):
    """
    Parses title, description, and modification date from the RDF/XML metadata.

    Args:
        xml_content (bytes): Raw XML metadata content.

    Returns:
        dict: Parsed metadata fields.
    """
    try:
        root = ET.fromstring(xml_content)
        namespace = {"dct": "http://purl.org/dc/terms/", "dcat": "http://www.w3.org/ns/dcat#"}
        title = root.find(".//dct:title", namespace)
        description = root.find(".//dct:description", namespace)
        modified = root.find(".//dct:modified", namespace)
        return {
            "Title": title.text if title is not None else "N/A",
            "Description": description.text if description is not None else "N/A",
            "Modified": modified.text if modified is not None else "N/A"
        }
    except ET.ParseError as e:
        log_error("Failed to parse XML metadata", "error", e)
        return {"Title": "N/A", "Description": "N/A", "Modified": "N/A"}

def remove_blank_node_ids(elem):
    """
    Recursively removes attributes that are blank node IDs from the XML tree.

    Args:
        elem (Element): XML element to clean.
    """
    for attr in list(elem.attrib):
        if attr.endswith("nodeID"):
            del elem.attrib[attr]
    for child in elem:
        remove_blank_node_ids(child)

def remove_license_elements(elem):
    """
    Recursively removes license-related elements from the XML tree.

    Args:
        elem (Element): XML element to clean.
    """
    elem[:] = [child for child in elem if not (
        child.tag.endswith("LicenseDocument") or child.tag.endswith("RightsStatement")
    )]
    for child in elem:
        remove_license_elements(child)

def sort_xml(elem):
    """
    Recursively sorts XML elements and attributes for consistent ordering.

    Args:
        elem (Element): Root XML element to sort.
    """
    elem.attrib = dict(sorted(elem.attrib.items()))
    for child in elem:
        sort_xml(child)
    elem[:] = sorted(
        elem,
        key=lambda e: (
            e.tag,
            sorted(e.attrib.items()),
            ET.tostring(e, encoding='utf-8', method='xml')
        )
    )

def prettify_xml(elem):
    """
    Formats an XML element into a pretty-printed string.

    Args:
        elem (Element): Root XML element.

    Returns:
        str: Pretty-printed XML string.
    """
    rough_string = ET.tostring(elem, encoding='utf-8')
    parsed = minidom.parseString(rough_string)
    return parsed.toprettyxml(indent="  ", newl="\n")

def download_opendata_swiss_xml():
    """
    Orchestrates downloading and processing of XML metadata for datasets listed in CSV.
    Saves cleaned and sorted XML files to disk.
    """
    data_file = "01_ETL/01_opendata.swiss/opendata_swiss_datasets.csv"
    save_folder = "01_ETL/01_opendata.swiss/saved_metadata_xml"
    os.makedirs(save_folder, exist_ok=True)
    identifiers = []
    if os.path.exists(data_file):
        with open(data_file, "r", encoding="utf-8") as file:
            identifiers = [line.strip() for line in file.readlines()[1:]]
    if MAX_DATASETS is not None:
        identifiers = identifiers[:MAX_DATASETS]

    log_error(f"Starting download of {len(identifiers)} dataset metadata files...", "info")
    for index, identifier in enumerate(identifiers, start=1):
        try:
            xml_data = fetch_xml_metadata(identifier)
            if xml_data:
                metadata = parse_xml_metadata(xml_data)
                root = ET.fromstring(xml_data)
                remove_blank_node_ids(root)
                remove_license_elements(root)
                sort_xml(root)
                sort_xml(root)  # Ensure full document-level sorting
                sorted_xml = prettify_xml(root)
                sorted_xml = "\n".join([line for line in sorted_xml.splitlines() if line.strip()])
                xml_filename = os.path.join(save_folder, f"{identifier}.xml")
                with open(xml_filename, "w", encoding="utf-8") as file:
                    file.write(sorted_xml)
                log_portal_result(PORTAL_NAME, "Download XML Metadata", success=True)
        except Exception as e:
            log_error(f"Failed to process {identifier}", "error", e)
            log_portal_result(PORTAL_NAME, "Download XML Metadata", success=False)
    log_error(f"All XML metadata files have been successfully saved in {save_folder}", "info")
    save_statistics()
