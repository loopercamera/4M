import os
import re
import xml.etree.ElementTree as ET
from functions.error_logger import log_error  # Import log_error from error_logger.py
from functions.statistics_logger import log_portal_result, save_statistics  # Import statistics tracking

# Counters for successful and failed XML files
success_count = 0
error_count = 0

# Define the portal name for tracking (Update as needed)
PORTAL_NAME = "opendata.swiss"  # Change this dynamically based on the process

def remove_html_tags(text):
    """Remove HTML tags from a string while preserving its content."""
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

def clean_xml_file(file_path):
    """Clean an XML file by removing HTML tags and formatting it properly."""
    global success_count, error_count  # Access the counters

    if not os.path.exists(file_path):
        log_error(f"File not found: {file_path}", "error")
        log_portal_result(PORTAL_NAME, "Clean XML Files", success=False)
        error_count += 1
        return

    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
    except ET.ParseError as e:
        log_error(f"XML parsing failed for {file_path}", "error", e)
        log_portal_result(PORTAL_NAME, "Clean XML Files", success=False)
        error_count += 1
        return
    except Exception as e:
        log_error(f"Unexpected error while parsing {file_path}", "error", e)
        log_portal_result(PORTAL_NAME, "Clean XML Files", success=False)
        error_count += 1
        return

    try:
        for elem in root.iter():
            if elem.text:
                elem.text = remove_html_tags(elem.text)
    except Exception as e:
        log_error(f"Error while processing text in {file_path}", "error", e)
        log_portal_result(PORTAL_NAME, "Clean XML Files", success=False)
        error_count += 1
        return

    try:
        cleaned_xml = ET.tostring(root, encoding='utf-8').decode('utf-8')
        cleaned_xml = cleaned_xml.replace('\n', '').replace('\t', '')
        cleaned_xml = re.sub(r"(&#13;|&#10;|&#xD;|&#xA;)", " ", cleaned_xml)

        cleaned_lines = []
        temp_line = ""

        for line in cleaned_xml.splitlines():
            line = line.strip()
            if not line.endswith(">"):
                temp_line += line + " "
            else:
                temp_line += line
                cleaned_lines.append(temp_line.strip())
                temp_line = ""

        if temp_line:
            cleaned_lines.append(temp_line.strip())

        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(cleaned_lines) + "\n")

        success_count += 1  # Increment success count
        log_portal_result(PORTAL_NAME, "Clean XML Files", success=True)

    except Exception as e:
        log_error(f"Error writing cleaned XML for {file_path}", "error", e)
        log_portal_result(PORTAL_NAME, "Clean XML Files", success=False)
        error_count += 1

def process_folder(folder_path):
    """Process all XML files in a specified folder."""
    global success_count, error_count

    if not isinstance(folder_path, str):
        log_error(f"Expected a string path, but got {type(folder_path)}: {folder_path}", "error")
        return

    if not os.path.exists(folder_path):
        log_error(f"Warning: Folder '{folder_path}' not found. Skipping...", "error")
        return

    xml_files = [f for f in os.listdir(folder_path) if f.endswith(".xml")]
    if not xml_files:
        log_error(f"No XML files found in '{folder_path}'. Skipping...", "info")
        return

    for filename in xml_files:
        file_path = os.path.join(folder_path, filename)
        clean_xml_file(file_path)


def process_folder(folder_path):
    """Process all XML files in a specified folder."""
    global success_count, error_count

    if not os.path.exists(folder_path):
        log_error(f"Folder '{folder_path}' not found. Skipping...", "warning")
        return

    xml_files = [f for f in os.listdir(folder_path) if f.endswith(".xml")]
    
    if not xml_files:
        log_error(f"No XML files found in '{folder_path}'. Skipping...", "info")
        return

    for filename in xml_files:
        file_path = os.path.join(folder_path, filename)
        clean_xml_file(file_path)
