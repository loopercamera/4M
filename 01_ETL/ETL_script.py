# ### V1
# ### Gross
# ### 19.03.2025

# # -------------------------------
# # 0. Start process
# # -------------------------------
from error_logger import log_start_message
log_start_message()

# # # -------------------------------
# # # 1. Process opendata.swiss
# # # -------------------------------

# # # 1.1 Download metadata
import download_opendata_swiss
download_opendata_swiss.gather_opendata_swiss_datasets()
download_opendata_swiss.download_opendata_swiss_xml()

# # # # 1.2 Clean metadata
import xml_cleaner
xml_cleaner.PORTAL_NAME = "opendata.swiss"
xml_cleaner.process_folders([
    r"01_ETL\01_opendata.swiss\saved_metadata_xml"
])

# # # 1.3 Compare metadata
from dataset_change_detector import compare_dataset_hashes
compare_dataset_hashes(
    base_dir=r"01_ETL\01_opendata.swiss",
    remove_order_file=r"01_ETL\removeorder_metadata_opendata.swiss.csv",
    extension="xml",
    portal_name="opendata.swiss"
)

# # # 1.4 Extract and export metadata
from extract_metadata_opendata_swiss import extract_and_save_all_opendata_swiss
extract_and_save_all_opendata_swiss(
    folder_path=r"01_ETL\01_opendata.swiss\saved_metadata_xml",
    output_folder=r"01_ETL\01_opendata.swiss"
)

# # # # 1.5 Transform extracted metadata
from transform_metadata_opendata_swiss import transform_opendata_swiss
transform_opendata_swiss(r"01_ETL\01_opendata.swiss")

# # -------------------------------
# # 2. Process geocat.ch
# # -------------------------------

<<<<<<< Updated upstream
# 2.1 Download metadata
from download_geocat import download_geocat_metadata
download_geocat_metadata(
    save_dir=r"01_ETL\02_geocat.ch\saved_metadata_xml",
    batch_size=100,
    wait_time=1,
    max_records=None  # Set to None to download everything
)
=======
# # 2.1 Download metadata
# # from download_geocat import download_geocat_metadata
# # download_geocat_metadata(
# #     save_dir=r"01_ETL\02_geocat.ch\saved_metadata_xml",
# #     batch_size=100,
# #     wait_time=1,
# #     max_records=None  # Set to None to download everything
# # )
>>>>>>> Stashed changes

# # 2.2 Clean metadata
# # xml_cleaner.PORTAL_NAME = "geocat.ch"
# # xml_cleaner.process_folders([
# #     r"01_ETL\02_geocat.ch\saved_metadata_xml"
# # ])

# # 2.3 Compare metadata
# # compare_dataset_hashes(
# #     base_dir=r"01_ETL\02_geocat.ch",
# #     remove_order_file=r"01_ETL\removeorder_metadata_geocat.ch.csv",
# #     extension="xml",
# #     portal_name="geocat.ch"
# # )

# # 2.4 Extract and export metadata
# # from extract_metadata_geocat import extract_and_save_all_geocat
# # extract_and_save_all_geocat(
# #     input_folder=r"01_ETL\02_geocat.ch\saved_metadata_xml",
# #     output_folder=r"01_ETL\02_geocat.ch"
# # )

# # 2.5 Transform extracted metadata
# # from transform_metadata_geocat import process_dataset_metadata, clean_csv_file

# # files = [
# #     r"01_ETL\02_geocat.ch\geocat_dataset_metadata.csv",
# #     r"01_ETL\02_geocat.ch\geocat_distribution_metadata.csv",
# #     r"01_ETL\02_geocat.ch\geocat_contact_point_metadata.csv"
# # ]

# # for f in files:
# #     clean_csv_file(f)
# #     process_dataset_metadata(f)

# # -------------------------------
# # 3. Merge All Metadata
# # -------------------------------
from merge_metadatafiles import merge_all_metadata

folders=[
        r"01_ETL\01_opendata.swiss",
        r"01_ETL\02_geocat.ch"
    ]
output_dir=r"01_ETL\11_group"

merge_all_metadata(folders, output_dir)




# # -------------------------------
# # 4. Load
# # -------------------------------

from load_metadata import (
    database_exists, reset_database,delete_from_csv_list,
    create_database,
    load_metadata
)



db_name = "4M"
folder_path = r"01_ETL\11_group"
config_path = r"01_ETL\21_load\db_config.json"
sql_path = r"01_ETL\21_load\create_db_script.sql"
csv_remove_path = r"01_ETL\removeorder_metadata_opendata.swiss.csv"

if database_exists(db_name, config_path):
    print(f"Database '{db_name}' exists.")
else:
    create_database(db_name, config_path,sql_path)

# # Delete metadata entries listed in CSV
delete_from_csv_list(csv_remove_path, db_name, config_path)






# # Import new data

# # DEV reset (delete & recreate DB)
# # reset_database(db_name, config_path,sql_path)

# # Load metadata (set overwrite_db to True or False)
load_metadata(folder_path, config_path, sql_path)

