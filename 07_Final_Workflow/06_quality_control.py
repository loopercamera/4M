# -------------------------------
# Parameters
# -------------------------------

confing_file_path_db = r"07_Final_Workflow\data\db_config.json"
db_name = "4M_test_finaler_workflow"


# -------------------------------
# Quality control
# -------------------------------

from functions.quality_control import set_quality_indicators

set_quality_indicators(
    config_file=confing_file_path_db,
    dbname=db_name,
    language_prefixes=["DE", "EN", "FR", "IT"],
    table_set_types=["dataset", "distribution"],
    format_lockup=r"07_Final_Workflow\data\formats_lockup.csv",
    limit=None
)

