# -------------------------------
# Parameters
# -------------------------------

confing_file_path_db = r"06_Final_Workflow\resources\db_config.json"
db_name = "4M"


# -------------------------------
# Quality control
# -------------------------------

from functions.quality_control import set_quality_indicators

set_quality_indicators(
    config_file=confing_file_path_db,
    dbname=db_name,
    language_prefixes=["DE", "EN", "FR", "IT"],
    table_set_types=["dataset", "distribution"],
    format_lockup=r"06_Final_Workflow\resources\formats_lockup.csv",
    limit=None
)

