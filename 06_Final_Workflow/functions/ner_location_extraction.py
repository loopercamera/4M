import psycopg2
import pandas as pd
import json
import re
from functions.error_logger import log_error


# Specify which columns should be checked for location data
LOCATION_COLUMNS = [
    "dataset_title_de",
    "dataset_description_de",
    "dataset_title_en",
    "dataset_description_en",
    "dataset_title_fr",
    "dataset_description_fr",
    "dataset_title_it",
    "dataset_description_it",
    "dataset_title_rm",
    "dataset_description_rm",
    "dataset_publisher_name",
    "dataset_identifier"
]

def load_config(config_path):
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_error("Failed to load configuration file:", level="error", exception=e)
        return None


def load_labels(label_data_path):
    try:
        with open(label_data_path, "r", encoding="utf-8") as f:
            categorized_labels = json.load(f)

        label_lookup = {entry["label"]: entry for entry in categorized_labels}

        # --- Preferred regex pattern based on label list ---
        label_list = sorted([entry["label"] for entry in categorized_labels], key=len, reverse=True)
        pattern = r'(?<!\w)(' + '|'.join(re.escape(label) for label in label_list) + r')(?!\w)'
        label_regex = re.compile(pattern)

        return label_lookup, label_regex

    except Exception as e:
        log_error("Failed to load label data:", level="error", exception=e)
        return None, None




def find_matches_with_meta(text, label_lookup, label_regex):
    if not isinstance(text, str):
        text = "" if text is None else str(text)
    matches = []
    for match in label_regex.finditer(text):
        label_text = match.group()
        metadata = label_lookup.get(label_text)
        if metadata:
            matches.append({
                "text": label_text,
                "label_id": metadata.get("label_id"),
                "level": metadata.get("level"),
                "canton": metadata.get("canton"),
                "district": metadata.get("district")
            })
    return matches



def extract_location_info(text, label_lookup, label_regex, source_text_id=None):
    matches = find_matches_with_meta(text or "", label_lookup, label_regex)

    if len(matches) == 1:
        return {"label_id": matches[0].get("label_id")}

    if len(matches) > 1:
        label_ids = {m.get("label_id") for m in matches}
        if len(label_ids) == 1:
            return {"label_id": matches[0].get("label_id")}

        districts = [m.get("district") for m in matches]
        unique_districts = set(districts)
        if None not in districts and len(unique_districts) == 1:
            return {
                "label_id": next(
                    m.get("label_id") for m in matches
                    if m.get("district") == list(unique_districts)[0]
                )
            }

        cantons = {m.get("canton") for m in matches if m.get("canton")}
        if len(cantons) == 1:
            canton_value = list(cantons)[0]
            for m in matches:
                if m.get("level") == 1 and m.get("canton") == canton_value:
                    return {"label_id": m.get("label_id")}

        levels = [m.get("level", float("inf")) for m in matches]
        min_level = min(levels)
        lowest_level_matches = [m for m in matches if m.get("level", float("inf")) == min_level]
        if len(lowest_level_matches) == 1:
            return {"label_id": lowest_level_matches[0].get("label_id")}

        if all("CH" in (m.get("label_id") or "") for m in matches):
            return {"label_id": "CH0000000000"}

        neighbor_fallbacks = {
            "AT": "AT0000000000",
            "DE": "DE0000000000",
            "FR": "FR0000000000",
            "IT": "IT0000000000",
            "LI": "LI0000000000",
        }
        for country_code, fallback_id in neighbor_fallbacks.items():
            if all(country_code in (m.get("label_id") or "") for m in matches):
                return {"label_id": fallback_id}

    return {"label_id": None}


def fetch_all_metadata_fields(config, limit=100):
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        # --- Basiskonfiguration der Abfrage ---
        query = """
        SELECT
            dataset_identifier,
            dataset_publisher_name,
            dataset_language,
            dataset_title_de,
            dataset_description_de,
            dataset_title_en,
            dataset_description_en,
            dataset_title_fr,
            dataset_description_fr,
            dataset_title_it,
            dataset_description_it,
            dataset_title_rm,
            dataset_description_rm
        FROM merged_dataset_metadata
        WHERE dataset_language IS NOT NULL AND dataset_location IS NULL
        """

        # --- Optionales Limit anhängen ---
        if limit is not None:
            query += f" LIMIT {int(limit)}"

        cur.execute(query)
        rows = cur.fetchall()

        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=colnames)

        cur.close()
        conn.close()

        if not df.empty:
            log_error("Successfully loaded data for location extraction", level="info")

        return df
    

    except Exception as e:
        log_error("Error loading metadata for for location extraction","error", e)
        return pd.DataFrame()
    

def update_location_fields_in_db(df_results, config):
    """
    Updates the location fields in the database for each dataset_identifier based on the provided DataFrame or list of dicts.

    Parameters:
    - df_results (DataFrame or list of dicts): Location info and dataset_identifier
    - config (dict): Database connection configuration
    """
    print("Start loading")
    if isinstance(df_results, list):
        df_results = pd.DataFrame(df_results)

    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        for _, row in df_results.iterrows():
            location_id = row.get("label_id")
            location = row.get("dataset_location")
            district = row.get("dataset_location_district")
            canton = row.get("dataset_location_canton")
            country = row.get("dataset_location_country")
            identifier = row.get("dataset_identifier")

            if pd.notna(identifier):
                query = """
                    UPDATE merged_dataset_metadata
                    SET
                        dataset_location_id = %s,
                        dataset_location = %s,
                        dataset_location_district = %s,
                        dataset_location_canton = %s,
                        dataset_location_country = %s
                    WHERE dataset_identifier = %s
                """
                values = (location_id, location, district, canton, country, identifier)
                cur.execute(query, values)

        conn.commit()
        cur.close()
        conn.close()
        log_error(f"Successfully added {len(df_results)} locations in merged_dataset_metadata.",level="info")
    except Exception as e:
        log_error(f"Database update failed:", level="error",exception=e)




import pandas as pd

def extract_label_matches(
    df,
    language_prefixes,
    label_lookup,
    label_regex,
    fallback_label_id="no_location_found"
) -> pd.DataFrame:
    results = []


    for _, row in df.iterrows():
        label_id = None
        match_field = None
        match_level = None
        for lang in language_prefixes:
            lang = lang.lower()

            fields = [
                f"dataset_title_{lang}",
                f"dataset_description_{lang}",
                "dataset_publisher_name",
                "dataset_identifier"
            ]



            for field in fields:
                if field not in row:
                    continue
                text = row.get(field, "")
                matches = find_matches_with_meta(text, label_lookup, label_regex)
                result = extract_location_info(text, label_lookup, label_regex, source_text_id=row.name)

                if result["label_id"]:
                    label_id = result["label_id"]
                    match_field = field
                    match_level = next((m["level"] for m in matches if m["label_id"] == label_id), None)
                    break
            if label_id:
                break

        results.append({
            "dataset_identifier": row["dataset_identifier"],
            "dataset_language": row["dataset_language"],
            "dataset_publisher_name": row["dataset_publisher_name"],
            "label_id": label_id if label_id else fallback_label_id,
            "match_field": match_field,
            "match_level": match_level
        })

    return pd.DataFrame(results)

import json
import pandas as pd

def add_location_columns(df: pd.DataFrame, label_map_path: str, fallback_label_id="no_location_found") -> pd.DataFrame:
    try:
        with open(label_map_path, "r", encoding="utf-8") as f:
            label_map_entries = json.load(f)
        

        # --- Lookups ---
        label_id_to_label = {entry["label_id"]: entry["label"] for entry in label_map_entries}
        label_id_to_district = {entry["label_id"]: entry.get("district") for entry in label_map_entries}
        label_id_to_canton = {entry["label_id"]: entry.get("canton") for entry in label_map_entries}

        # --- Copy DataFrame ---
        df = df.copy()

        # --- Add columns from lookups ---
        df["dataset_location"] = df["label_id"].map(label_id_to_label)
        df["dataset_location_district"] = df["label_id"].map(label_id_to_district)
        df["dataset_location_canton"] = df["label_id"].map(label_id_to_canton)

        # --- Add country only if label_id is valid ---
        def extract_country(label_id):
            if label_id and label_id != fallback_label_id:
                return str(label_id)[:2]
            return "not_found"

        df["dataset_location_country"] = df["label_id"].apply(extract_country)

        # --- Fill missing values ---
        df["dataset_location"] = df["dataset_location"].fillna("not_found")
        df["dataset_location_district"] = df["dataset_location_district"].fillna("not_found")
        df["dataset_location_canton"] = df["dataset_location_canton"].fillna("not_found")
        df["dataset_location_country"] = df["dataset_location_country"].fillna("not_found")

        return df

    except Exception as e:
        log_error("Failed to enrich DataFrame with location columns:", level="error", exception=e)
        return df



def ner_extraction_locations(config_file, dbname, language_prefixes, label_data_path, label_map_path, limit=100):
    config = load_config(config_file)
    config["dbname"] = dbname

    label_lookup, label_regex = load_labels(label_data_path)

    df_data = fetch_all_metadata_fields(config, limit=limit)
    if df_data.empty:
        log_error("No data found to process in database for location extraction. Stopping location extraction.", level="warning")
        return pd.DataFrame()

    df_extracted_labels = extract_label_matches(df_data, language_prefixes, label_lookup, label_regex)
    df_matched_locations = add_location_columns(df_extracted_labels, label_map_path)

    update_location_fields_in_db(df_matched_locations, config)

    return pd.DataFrame(df_matched_locations)




