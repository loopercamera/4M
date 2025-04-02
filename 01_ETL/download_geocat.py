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
    return re.sub(r'[<>:"/\\|?*]', '_', identifier)

def get_total_record_count():
    # Use a known working schema and request setup
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
        "maxRecords": 1,
        "startPosition": 1,
        "sortBy": "title:A"
    }
    try:
        prepared = requests.Request('GET', CSW_URL, params=params).prepare()
        print("Request URL (get_total_record_count):", prepared.url)
        response = requests.get(CSW_URL, params=params)
        response.raise_for_status()
        root = ET.fromstring(response.text)
        search_results = root.find(".//{http://www.opengis.net/cat/csw/2.0.2}SearchResults")
        if search_results is not None:
            return int(search_results.attrib.get("numberOfRecordsMatched", 0))
        else:
            log_error("SearchResults not found in get_total_record_count.")
            log_error(f"Response content:\n{response.text[:1000]}...")
            return 0
    except Exception as e:
        log_error("Failed to fetch total record count.", exception=e)
        return 0

def fetch_records(start_position, max_records):
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
        prepared = requests.Request('GET', CSW_URL, params=params).prepare()
        print("Request URL (fetch_records):", prepared.url)
        response = requests.get(CSW_URL, params=params)
        response.raise_for_status()
        xml_text = response.text

        root = ET.fromstring(xml_text)

        # Check for ExceptionReport
        exception = root.find(".//{http://www.opengis.net/ows}ExceptionText")
        if exception is not None:
            log_error("Exception in response: " + exception.text.strip())
            log_error(f"Problematic XML (start_position={start_position}): {xml_text[:1000]}...")
            return xml_text, 0, 0

        search_results = root.find(".//{http://www.opengis.net/cat/csw/2.0.2}SearchResults")
        if search_results is None:
            log_error("SearchResults element not found in response.")
            log_error(f"Problematic XML (start_position={start_position}): {xml_text[:1000]}...")
            return xml_text, 0, 0

        num_returned = int(search_results.attrib.get("numberOfRecordsReturned", 0))
        num_matched = int(search_results.attrib.get("numberOfRecordsMatched", 0))

        return xml_text, num_returned, num_matched
    except Exception as e:
        log_error("Failed to fetch records from GeoCat CSW.", exception=e)
        return "", 0, 0

def extract_and_save_each_metadata(xml_text, save_dir):
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
    os.makedirs(save_dir, exist_ok=True)
    print("Starting download of metadata records...")

    total_available = get_total_record_count()
    print(f"Total number of records available: {total_available}")

    if max_records is None or max_records > total_available:
        max_records = total_available

    start = 1
    total_saved = 0

    while start <= max_records:
        print(f"Fetching record {start}...")
        xml_data, num_returned, _ = fetch_records(start_position=start, max_records=1)
        if num_returned == 0:
            print("No record returned or an error occurred. Skipping.")
        else:
            saved = extract_and_save_each_metadata(xml_data, save_dir)
            total_saved += saved

        start += 1
        time.sleep(wait_time)

    print(f"Done. Total records saved: {total_saved}")
    log_error(f"Download completed. Total records saved: {total_saved}", level="info")
    log_portal_result("GeoCat", "Download Metadata", success=total_saved)
    save_statistics()

if __name__ == "__main__":
    download_geocat_metadata()