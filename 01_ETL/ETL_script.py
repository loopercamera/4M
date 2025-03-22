### V1
### Gross
### 19.03.2025

# -------------------------------
# 0. Start process
# -------------------------------
from error_logger import log_start_message
log_start_message()

# -------------------------------
# 1. Process opendata.swiss
# -------------------------------

# 1.1 Download metadata
import download_opendata_swiss
download_opendata_swiss.gather_opendata_swiss_datasets()
download_opendata_swiss.download_opendata_swiss_xml()

# 1.2 Clean metadata
import xml_cleaner
xml_cleaner.PORTAL_NAME = "opendata.swiss"
xml_cleaner.process_folders([
    r"01_ETL\01_opendata.swiss\saved_metadata_xml"
])

# 1.3 Compare metadata
from dataset_change_detector import compare_dataset_hashes
compare_dataset_hashes(
    base_dir=r"C:\FHNW_lokal\6000\4M\01_ETL\01_opendata.swiss",
    remove_order_file=r"C:\FHNW_lokal\6000\4M\01_ETL\removeorder_metadata_opendata.swiss.csv",
    extension="xml",
    portal_name="opendata.swiss"
)

# 1.4 Extract and export metadata
from extract_metadata_opendata_swiss import extract_and_save_all
extract_and_save_all(
    folder_path=r"01_ETL\01_opendata.swiss\saved_metadata_xml",
    output_folder=r"01_ETL\01_opendata.swiss"
)

# 1.5 Transform extracted metadata
from transform_metadata_opendata_swiss import transform_opendata_swiss
transform_opendata_swiss(r"01_ETL\01_opendata.swiss")

# -------------------------------
# 2. Process geocat.ch
# -------------------------------

# 2.1 Download metadata
from download_geocat import download_geocat_metadata
download_geocat_metadata(
    save_dir=r"01_ETL\02_geocat.ch\saved_metadata_xml",
    batch_size=100,
    wait_time=1,
    max_records=100  # Set to None to download everything
)

# 2.2 Clean metadata
xml_cleaner.PORTAL_NAME = "geocat.ch"
xml_cleaner.process_folders([
    r"01_ETL\02_geocat.ch\saved_metadata_xml"
])

# 2.3 Compare metadata
compare_dataset_hashes(
    base_dir=r"01_ETL\02_geocat.ch",
    remove_order_file=r"01_ETL\removeorder_metadata_geocat.ch.csv",
    extension="xml",
    portal_name="geocat.ch"
)

# 2.4 Extract and export metadata
from extract_metadata_geocat import extract_and_save_all
extract_and_save_all(
    input_folder=r"01_ETL\02_geocat.ch\saved_metadata_xml",
    output_folder=r"01_ETL\02_geocat.ch"
)

# 2.5 Transform extracted metadata
from transform_metadata_geocat import process_dataset_metadata, clean_csv_file

files = [
    r"01_ETL\02_geocat.ch\geocat_dataset_metadata.csv",
    r"01_ETL\02_geocat.ch\geocat_distribution_metadata.csv",
    r"01_ETL\02_geocat.ch\geocat_contact_point_metadata.csv"
]

process_dataset_metadata(files[0])
for f in files:
    clean_csv_file(f)

# -------------------------------
# 3. Merge All Metadata
# -------------------------------
from merge_metadatafiles import merge_all_metadata

merge_all_metadata(
    folders=[
        r"01_ETL\01_opendata.swiss",
        r"01_ETL\02_geocat.ch"
    ],
    output_dir=r"01_ETL\11_group"
)
