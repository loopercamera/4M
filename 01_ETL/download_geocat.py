import requests
import xml.etree.ElementTree as ET
import os
import time
import re
from error_logger import log_error, log_start_message
from statistics_logger import log_portal_result, save_statistics

# CSW endpoint and schema
CSW_URL = "https://www.geocat.ch/geonetwork/srv/deu/csw"
SCHEMA = "http://www.geocat.ch/2008/che"

def sanitize_filename(identifier):
    """
    Replace invalid filename characters with underscores.

    Args:
        identifier (str): The original identifier string.

    Returns:
        str: A sanitized filename-safe string.
    """
    return re.sub(r'[<>:"/\\|?*]', '_', identifier)

def fetch_records(start_position, max_records=100):
    """
    Fetch CSW records from the GeoCat endpoint.

    Args:
        start_position (int): The starting record position.
        max_records (int): Number of records to fetch.

    Returns:
        str: Raw XML response as a string.
    """
    params = {
        "service": "CSW",
        "version": "2.0.2",
        "request": "GetRecords",
        "namespace": "xmlns(csw=http://www.opengis.net/cat/csw/2.0.2)",
        "typeNames": "csw:Record",
        "elementSetName": "full",
        "resultType": "results",
        "outputFormat": "application/xml",
        "outputSchema": SCHEMA,
        "maxRecords": max_records,
        "startPosition": start_position,
        "sortBy": "title:A"
    }

    try:
        response = requests.get(CSW_URL, params=params)
        response.raise_for_status()
        return response.text
    except Exception as e:
        log_error("Failed to fetch records from GeoCat CSW.", exception=e)
        raise

def extract_and_save_each_metadata(xml_text, save_dir):
    """
    Extract metadata records from XML and save them as individual XML files.

    Args:
        xml_text (str): XML string returned from CSW.
        save_dir (str): Directory to save individual XML metadata files.

    Returns:
        int: Number of records successfully saved.
    """
    namespaces = {
        "che": "http://www.geocat.ch/2008/che",
        "gmd": "http://www.isotc211.org/2005/gmd",
        "gco": "http://www.isotc211.org/2005/gco"
    }
    try:
        root = ET.fromstring(xml_text)
        records = root.findall(".//che:CHE_MD_Metadata", namespaces)
    except Exception as e:
        log_error("Failed to parse XML metadata.", exception=e)
        return 0

    saved_count = 0
    for record in records:
        try:
            identifier_el = record.find(".//gmd:fileIdentifier/gco:CharacterString", namespaces)
            identifier = identifier_el.text.strip() if identifier_el is not None else None
            if identifier:
                safe_name = sanitize_filename(identifier)
                filename = os.path.join(save_dir, f"{safe_name}.xml")
                with open(filename, "w", encoding="utf-8") as f:
                    xml_str = ET.tostring(record, encoding="unicode")
                    f.write(xml_str)
                saved_count += 1
            else:
                log_error("Skipping record with missing identifier.")
        except Exception as e:
            log_error("Failed to save individual metadata record.", exception=e)
    return saved_count

def download_geocat_metadata(save_dir="saved_metadata_xml", batch_size=100, wait_time=1, max_records=None):
    """
    Download all available metadata records from GeoCat CSW and save as XML files.

    Args:
        save_dir (str): Directory to save the XML files.
        batch_size (int): Number of records to request per CSW batch.
        wait_time (int): Delay in seconds between requests.
        max_records (int or None): Maximum number of records to fetch. If None, fetch all.
    """
    os.makedirs(save_dir, exist_ok=True)
    print("Starting download of metadata records...")
    start = 1
    total_saved = 0

    while True:
        if max_records is not None and total_saved >= max_records:
            print(f"Reached MAX_RECORDS = {max_records}. Stopping.")
            break

        print(f"Fetching records {start} to {start + batch_size - 1}...")
        try:
            xml_data = fetch_records(start_position=start, max_records=batch_size)
            saved = extract_and_save_each_metadata(xml_data, save_dir)
            if saved == 0:
                print("No more records found. Stopping.")
                break
            total_saved += saved
            start += batch_size
            time.sleep(wait_time)
        except Exception as e:
            log_error("Error during metadata download process.", exception=e)
            log_portal_result("GeoCat", "Download Metadata", success=False)
            save_statistics()
            break

    print(f"Done. Total records saved: {total_saved}")
    log_error(f"Download completed. Total records saved: {total_saved}", level="info")
    log_portal_result("GeoCat", "Download Metadata", success=total_saved)
    save_statistics()

if __name__ == "__main__":
    download_geocat_metadata()