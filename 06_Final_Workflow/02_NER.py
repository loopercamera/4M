# -------------------------------
# Parameters
# -------------------------------

confing_file_path_db = r"06_Final_Workflow\resources\db_config.json"
db_name = "4M"


# -------------------------------
# NER
# -------------------------------
from functions.ner_location_extraction import ner_extraction_locations


ner_extraction_locations(
    config_file=confing_file_path_db,
    dbname = db_name,
    language_prefixes=["de", "fr", "en", "it", "rm"],
    label_data_path=r"06_Final_Workflow\resources\gemeinden_labels.json",
    label_map_path=r"06_Final_Workflow\resources\gemeinden_label_mapping.json",
    limit=None
)
