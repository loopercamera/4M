# # -------------------------------
# # Parameters
# # -------------------------------

confing_file_path_db = r"07_Final_Workflow\data\db_config.json"
sql_path_db = r"07_Final_Workflow\data\create_db_script.sql"
db_name = "4M_test_finaler_workflow"


# # -------------------------------
# # 0. Start process
# # -------------------------------
from functions.error_logger import log_start_message
log_start_message()


# # # -------------------------------
# # # 1. Process opendata.swiss
# # # -------------------------------

# 1.1 Download metadata
from functions import download_opendata_swiss

opendata_swiss_base_dir = r"07_Final_Workflow\data\01_opendata.swiss"
opendata_swiss_list_datasets_save_path = opendata_swiss_base_dir + r"\opendata_swiss_datasets.csv"
open_data_swiss_data_folder = opendata_swiss_base_dir + r"\saved_metadata_xml"
MAX_DATASETS = 10

# download_opendata_swiss.gather_opendata_swiss_datasets(opendata_swiss_list_datasets_save_path)
# download_opendata_swiss.download_opendata_swiss_xml(opendata_swiss_list_datasets_save_path,open_data_swiss_data_folder,MAX_DATASETS)


# # # # 1.2 Clean metadata
from functions import xml_cleaner
xml_cleaner.PORTAL_NAME = "opendata.swiss"
xml_cleaner.process_folder(open_data_swiss_data_folder)

# # # 1.3 Compare metadata
from functions.dataset_change_detector import compare_dataset_hashes
compare_dataset_hashes(
    base_dir=opendata_swiss_base_dir,
    remove_order_file=opendata_swiss_base_dir + r"\removeorder_metadata_opendata.swiss.csv",
    extension="xml",
    portal_name="opendata.swiss"
)

# # 1.4 Extract and export metadata
from functions.extract_metadata_opendata_swiss import extract_and_save_all_opendata_swiss
extract_and_save_all_opendata_swiss(
    folder_path=open_data_swiss_data_folder,
    output_folder=opendata_swiss_base_dir
)

# 1.5 Transform extracted metadata
from functions.transform_metadata_opendata_swiss import transform_opendata_swiss
transform_opendata_swiss(opendata_swiss_base_dir)

# -------------------------------
# 2. Process geocat.ch
# -------------------------------

geocat_base_dir = r"07_Final_Workflow\data\02_geocat.ch"
geocat_data_folder = geocat_base_dir + r"\saved_metadata_xml"


# 2.1 Download metadata
# from functions.download_geocat import download_geocat_metadata
# download_geocat_metadata(
#     save_dir= geocat_base_dir,
#     csv_file= "geocat_dataset_id_title.csv",
#     log_file= "geocat_download_iteration.log",
#     start_pos=1,
#     batch_size=250
# )

# XML-Dateien einzeln aus der CSV herunterladen
# from functions.download_geocat import download_xml_metadata_from_csv
# download_xml_metadata_from_csv(
#     save_dir= geocat_base_dir,
#     csv_file="geocat_dataset_id_title.csv",
#     max_files= 10  # oder None für alle
# )



# 2.2 Clean metadata
from functions import xml_cleaner
xml_cleaner.PORTAL_NAME = "geocat.ch"
xml_cleaner.process_folder(geocat_data_folder)

# 2.3 Compare metadata
from functions.dataset_change_detector import compare_dataset_hashes
compare_dataset_hashes(
    base_dir=geocat_base_dir,
    remove_order_file= geocat_base_dir + r"\removeorder_metadata_geocat.ch.csv",
    extension="xml",
    portal_name="geocat.ch"
)

# 2.4 Extract and export metadata
from functions.extract_metadata_geocat import extract_and_save_all_geocat
extract_and_save_all_geocat(
    input_folder= geocat_data_folder,
    output_folder= geocat_base_dir
)

# 2.5 Transform extracted metadata
from functions.transform_metadata_geocat import process_dataset_metadata, clean_csv_file

files = [
    geocat_base_dir + r"\geocat_dataset_metadata.csv",
    geocat_base_dir + r"\geocat_distribution_metadata.csv",
    geocat_base_dir + r"\geocat_contact_point_metadata.csv"
]

for f in files:
    clean_csv_file(f)
    process_dataset_metadata(f)

# -------------------------------
# 3. Merge All Metadata
# -------------------------------
from functions.merge_metadatafiles import merge_all_metadata

folders=[
        opendata_swiss_base_dir,
        geocat_base_dir
    ]
output_dir=r"07_Final_Workflow\data\11_group"

merge_all_metadata(folders, output_dir)




# -------------------------------
# 4. Load
# -------------------------------


from functions.load_metadata import (
    database_exists, reset_database,delete_from_csv_list,
    create_database,
    load_metadata
)

db_name = db_name 
folder_path = r"07_Final_Workflow\data\11_group"
config_path = confing_file_path_db
sql_path = sql_path_db
csv_remove_paths = [
    opendata_swiss_base_dir + r"\removeorder_metadata_opendata.swiss.csv",
    geocat_base_dir + r"\removeorder_metadata_geocat.ch.csv",
]


if database_exists(db_name, config_path):
    print(f"Database '{db_name}' exists.")
else:
    create_database(db_name, config_path,sql_path)

# Import new data
load_metadata(folder_path, config_path, db_name)


# -------------------------------
# 5. Language Detection
# -------------------------------


from functions.language_correction import language_correction


language_correction(
    config_file = confing_file_path_db,
    dbname = db_name,
    table_name = "merged_dataset_metadata",
    language_prefixes = ["DE", "EN", "FR", "IT"],
    base_columns = ["dataset_title", "dataset_keyword", "dataset_description"],
    table_set_type = "dataset",
    min_length_lang_detect = 20
)


language_correction(
    config_file =  confing_file_path_db,
    dbname = db_name,
    table_name = "merged_distribution_metadata",
    language_prefixes = ["DE", "EN", "FR", "IT"],
    base_columns = ["distribution_title","distribution_description"],
    table_set_type = "distribution",
    min_length_lang_detect = 20
)


