# extract_metadata_opendata_swiss.py
"""
This module provides functionality to extract metadata from DCAT-AP-CH compliant XML files,
including dataset-level metadata, distribution information, and contact points.

It uses Python's ElementTree for XML parsing and pandas for organizing and exporting metadata to CSV files.

Functions:
- extract_multilang_elements: Extract multilingual fields like title, description, keywords.
- extract_text / extract_attribute / extract_list: Utility functions for safe XML value extraction.
- extract_identifier / extract_issued_date / extract_publisher: Specific metadata field extractors.
- extract_distributions: Extracts distribution metadata linked to a dataset.
- extract_contact_points: Extracts organization or individual contact metadata.
- extract_metadata_from_xml: Master function that parses and extracts metadata from a single XML file.
- extract_and_save_all: Batch processes all XML files in a folder and saves the resulting CSVs.

Run this file as a script to process metadata from the default "saved_metadata_xml/" folder and output results to base folder.
"""
from functions.error_logger import log_error  # Import here to make it safe in non-logging contexts
from functions.statistics_logger import log_portal_result, save_statistics
import xml.etree.ElementTree as ET
import pandas as pd
import os

def extract_multilang_elements(element_name, dataset_element, namespace, default_label):
    elements = {}
    collected_values = {}
    for element in dataset_element.findall(f".//{element_name}", namespace):
        lang_attr = element.get("{http://www.w3.org/XML/1998/namespace}lang", "unknown").upper()
        text_value = element.text.strip() if element.text else "N/A"
        collected_values.setdefault(lang_attr, []).append(text_value)
    for lang, values in collected_values.items():
        elements[f"{default_label}_{lang}"] = ", ".join(values)
    if not elements:
        elements[f"{default_label}_UNKNOWN"] = "N/A"
    return elements

def extract_text(element, tag, namespace, default="N/A"):
    found_element = element.find(tag, namespace)
    return found_element.text.strip() if found_element is not None and found_element.text else default

def extract_attribute(element, tag, attribute, namespace, default="N/A"):
    found_element = element.find(tag, namespace)
    return found_element.get(attribute, default) if found_element is not None else default

def extract_identifier(dataset_element, namespace):
    identifier_element = dataset_element.find("dct:identifier", namespace)
    return identifier_element.text if identifier_element is not None else "N/A"

def extract_list(element, tag, namespace):
    return [elem.text.strip() for elem in element.findall(tag, namespace) if elem.text] or ["N/A"]

def extract_issued_date(distribution_element, namespace):
    issued_element = distribution_element.find(".//dct:issued", namespace)
    return issued_element.text if issued_element is not None else "N/A"

def extract_publisher(dataset_element, namespace):
    publisher_element = dataset_element.find("dct:publisher/foaf:Organization", namespace)
    if publisher_element is not None:
        publisher_url = publisher_element.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about", "N/A")
        publisher_name_element = publisher_element.find("foaf:name", namespace)
        publisher_name = publisher_name_element.text.strip() if publisher_name_element is not None else "N/A"
    else:
        publisher_url, publisher_name = "N/A", "N/A"
    return {"dataset_publisher_url": publisher_url, "dataset_publisher_name": publisher_name}

def extract_distributions(dataset_element, namespace, dataset_id):
    distributions = []
    for distribution_element in dataset_element.findall(".//dcat:Distribution", namespace):
        access_url_element = distribution_element.find("dcat:accessURL", namespace)
        access_url = access_url_element.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", "N/A") if access_url_element is not None else "N/A"
        license_element = distribution_element.find("dct:license", namespace)
        license_url = license_element.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", "N/A") if license_element is not None else "N/A"
        rights_element = distribution_element.find("dct:rights", namespace)
        rights_text = rights_element.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", "N/A") if rights_element is not None else "N/A"
        byte_size_element = distribution_element.find("dcat:byteSize", namespace)
        byte_size = byte_size_element.text if byte_size_element is not None else "N/A"
        format_element = distribution_element.find("dct:format", namespace)
        format_url = format_element.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", "N/A") if format_element is not None else "N/A"
        media_type_element = distribution_element.find("dcat:mediaType", namespace)
        media_type = media_type_element.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", "N/A") if media_type_element is not None else "N/A"
        modified_element = distribution_element.find("dct:modified", namespace)
        modified_date = modified_element.text if modified_element is not None else "N/A"
        languages = [lang.text.strip() for lang in distribution_element.findall("dct:language", namespace) if lang.text] or ["N/A"]
        distribution_titles = extract_multilang_elements("dct:title", distribution_element, namespace, "distribution_title")
        distribution_descriptions = extract_multilang_elements("dct:description", distribution_element, namespace, "distribution_description")
        distribution_identifier_element = distribution_element.find("dct:identifier", namespace)
        distribution_identifier = distribution_identifier_element.text.strip() if distribution_identifier_element is not None else "N/A"
        download_url_element = distribution_element.find("dcat:downloadURL", namespace)
        download_url = download_url_element.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", "N/A") if download_url_element is not None else "N/A"
        documentation_element = distribution_element.find(".//foaf:page/foaf:Document", namespace)
        documentation_url = documentation_element.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about", "N/A") if documentation_element is not None else "N/A"
        distribution_temporal_resolution_element = distribution_element.find("dcat:temporalResolution", namespace)
        distribution_temporal_resolution = distribution_temporal_resolution_element.text if distribution_temporal_resolution_element is not None else "N/A"
        coverage_elements = [elem.text.strip() for elem in distribution_element.findall("{http://purl.org/dc/terms/}coverage") if elem.text]
        coverage = coverage_elements if coverage_elements else ["N/A"]
        distribution_entry = {
            "dataset_identifier": dataset_id,
            "distribution_id": distribution_element.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about", "N/A"),
            "distribution_issued_date": extract_issued_date(distribution_element, namespace),
            "distribution_modified_date": modified_date,
            "distribution_access_url": access_url,
            "distribution_license": license_url,
            "distribution_rights": rights_text,
            "distribution_byte_size": byte_size,
            "distribution_format": format_url,
            "distribution_media_type": media_type,
            "distribution_language": languages,
            "distribution_download_url": download_url,
            "distribution_coverage": coverage,
            "distribution_temporal_resolution": distribution_temporal_resolution,
            "distribution_documentation": documentation_url,
            "distribution_identifier": distribution_identifier,
            "origin": "opendata.swiss"
        }
        distribution_entry.update(distribution_titles)
        distribution_entry.update(distribution_descriptions)
        distributions.append(distribution_entry)
    return distributions

def extract_contact_points(dataset_element, namespace, dataset_id):
    contact_points = []
    for contact_element in dataset_element.findall(".//dcat:contactPoint", namespace):
        organization_element = contact_element.find("vcard:Organization", namespace)
        individual_element = contact_element.find("vcard:Individual", namespace)
        if organization_element is not None:
            contact_type = "Organization"
            email_element = organization_element.find("vcard:hasEmail", namespace)
            email = email_element.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", "N/A") if email_element is not None else "N/A"
            name_element = organization_element.find("vcard:fn", namespace)
            name = name_element.text.strip() if name_element is not None else "N/A"
        elif individual_element is not None:
            contact_type = "Individual"
            email_element = individual_element.find("vcard:hasEmail", namespace)
            email = email_element.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", "N/A") if email_element is not None else "N/A"
            name_element = individual_element.find("vcard:fn", namespace)
            name = name_element.text.strip() if name_element is not None else "N/A"
        else:
            continue
        contact_points.append({
            "dataset_identifier": dataset_id,
            "contact_type": contact_type,
            "contact_email": email,
            "contact_name": name,
            "origin": "opendata.swiss"
        })
    return contact_points

def extract_metadata_from_xml(xml_file, xml_filename):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    namespace = {
        "dct": "http://purl.org/dc/terms/",
        "foaf": "http://xmlns.com/foaf/0.1/",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "dcat": "http://www.w3.org/ns/dcat#",
        "vcard": "http://www.w3.org/2006/vcard/ns#",
        "xml": "http://www.w3.org/XML/1998/namespace",
        "schema": "http://schema.org/",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    }
    dataset_element = root.find(".//dcat:Dataset", namespace)
    if dataset_element is None:
        return {}, [], []
    dataset_id = extract_identifier(dataset_element, namespace)
    sorted_keywords = extract_multilang_elements("dcat:keyword", dataset_element, namespace, "dataset_keyword")
    dataset_descriptions = extract_multilang_elements("dct:description", dataset_element, namespace, "dataset_description")
    dataset_titles = extract_multilang_elements("dct:title", dataset_element, namespace, "dataset_title")
    distributions = extract_distributions(dataset_element, namespace, dataset_id)
    contact_points = extract_contact_points(dataset_element, namespace, dataset_id)
    dataset_language = [lang.text.strip() for lang in dataset_element.findall("dct:language", namespace) if lang.text] or ["N/A"]
    dataset_metadata = {
        "dataset_identifier": dataset_id,
        "origin": "opendata.swiss",
        "dataset_language" : dataset_language
    }
    dataset_metadata.update(sorted_keywords)
    dataset_metadata.update(dataset_descriptions)
    dataset_metadata.update(dataset_titles)
    return dataset_metadata, distributions, contact_points

def extract_and_save_all_opendata_swiss(folder_path, output_folder):
    
    log_error(f"Start extraction form opendata.swiss XML files", level="info")
    dataset_data = []
    distribution_data = []
    contact_point_data = []
    try:
        for filename in os.listdir(folder_path):
            if filename.endswith(".xml"):
                file_path = os.path.join(folder_path, filename)
                dataset_metadata, distributions, contact_points = extract_metadata_from_xml(file_path, filename)
                dataset_metadata["xml_filename"] = filename
                for d in distributions:
                    d["xml_filename"] = filename
                for c in contact_points:
                    c["xml_filename"] = filename
                dataset_data.append(dataset_metadata)
                distribution_data.extend(distributions)
                contact_point_data.extend(contact_points)

        df_dataset = pd.DataFrame(dataset_data)
        df_distribution = pd.DataFrame(distribution_data)
        df_contact_point = pd.DataFrame(contact_point_data)
        os.makedirs(output_folder, exist_ok=True)
        df_dataset.to_csv(os.path.join(output_folder, "opendata_dataset_metadata.csv"), index=False)
        df_distribution.to_csv(os.path.join(output_folder, "opendata_distribution_metadata.csv"), index=False)
        df_contact_point.to_csv(os.path.join(output_folder, "opendata_contact_point_metadata.csv"), index=False)

        log_error(f"All files from '{folder_path}' extracted and saved to '{output_folder}'.", level="info")
        log_error("Metadata extraction and CSV export completed successfully.", level="info")


        log_portal_result("opendata.swiss", "Files Extracted", success=len(dataset_data))
        log_portal_result("opendata.swiss", "Rows in Datasets CSV", success=len(df_dataset))
        log_portal_result("opendata.swiss", "Rows in Distributions CSV", success=len(df_distribution))
        log_portal_result("opendata.swiss", "Rows in Contact Points CSV", success=len(df_contact_point))
        save_statistics()

        return df_dataset, df_distribution, df_contact_point



    except Exception as e:
        log_error("An error occurred during metadata extraction.", level="error", exception=e)
        raise



if __name__ == "__main__":
    folder_path = "saved_metadata_xml"
    output_folder = ""

    try:
        df_dataset, df_distribution, df_contact_point = extract_and_save_all_opendata_swiss(folder_path, output_folder)

        print("Extracted Dataset Metadata:")
        print(df_dataset)
        print("Extracted Distribution Metadata:")
        print(df_distribution)

    except Exception as e:
        log_error("An error occurred during metadata extraction.", level="error", exception=e)
        raise

