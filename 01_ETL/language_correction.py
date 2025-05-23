
import psycopg2
import pandas as pd
import json
import langid
from tqdm import tqdm

langid.set_languages(['en', 'fr', 'de', 'it'])

def load_config(config_path):
    with open(config_path) as f:
        return json.load(f)

def generate_columns(identifier, base_cols, prefixes, table_type):
    language_columns = [f"{col}_{lang.lower()}" for lang in prefixes for col in base_cols]
    return [identifier] + language_columns

def process_language_status(df, prefixes, table_set_type):
    EMPTY_VALUES = {"", "nan", "none", "null"}

    def is_empty(val):
        return pd.isna(val) or str(val).strip().lower() in EMPTY_VALUES

    for lang in prefixes:
        title_col = f"{table_set_type.lower()}_title_{lang.lower()}"
        keyword_col = f"{table_set_type.lower()}_keyword_{lang.lower()}"
        desc_col = f"{table_set_type.lower()}_description_{lang.lower()}"
        status_col = f"language_status_{lang.lower()}"

        def check_status(row):
            title_val = row.get(title_col)
            keyword_val = row.get(keyword_col)
            desc_val = row.get(desc_col)
            if all(is_empty(val) for val in [title_val, keyword_val, desc_val]):
                return "no_data"
            if all(is_empty(val) for val in [title_val, desc_val]) and not is_empty(keyword_val):
                return "only_keywords"
            return None

        df[status_col] = df.apply(check_status, axis=1)
    return df

def detect_preferred_language(text, language_prefixes):
    language_prefixes = [lang.lower() for lang in language_prefixes]
    try:
        lang, prob = langid.classify(str(text))
        if lang in language_prefixes:
            return lang, round(prob, 3)
        else:
            return ['not_found'], 0.0
    except:
        return ['not_found'], 0.0

def evaluate_text_lengths(df, language_prefixes, min_length_lang_detect, table_set_type):
    for lang in language_prefixes:
        lang = lang.lower()
        title_col = f"{table_set_type.lower()}_title_{lang}"
        desc_col = f"{table_set_type.lower()}_description_{lang}"
        flag_col = f"text_length_flag_{lang}"
        status_col = f"language_status_{lang}"

        def assess_length(row):
            title = str(row[title_col]) if pd.notna(row[title_col]) else ""
            desc = str(row[desc_col]) if pd.notna(row[desc_col]) else ""
            title_long = len(title.strip()) > min_length_lang_detect
            desc_long = len(desc.strip()) > min_length_lang_detect
            if title_long and desc_long:
                return "title_and_description_long"
            elif desc_long:
                return "description_long"
            elif title_long:
                return "title_long"
            else:
                return "neither_long"

        df[flag_col] = df.apply(assess_length, axis=1)

        def validate_language(row):
            if row[flag_col] == "neither_long":
                if any(pd.notna(row[col]) and str(row[col]).strip() for col in [title_col, desc_col]):
                    return "not_enough_information"
                return row.get(status_col, None)
            if row[flag_col] in ["title_and_description_long", "description_long", "title_long"]:
                text_parts = []
                if row[flag_col] in ["title_and_description_long", "title_long"]:
                    text_parts.append(str(row[title_col]))
                if row[flag_col] in ["title_and_description_long", "description_long"]:
                    text_parts.append(str(row[desc_col]))
                combined_text = " ".join(text_parts)
                detected_lang, detected_prob = detect_preferred_language(combined_text, language_prefixes)
                if detected_lang == lang:
                    return f"correct"
                else:
                    return f"incorrect_new_{detected_lang}"
            return row.get(status_col, None)

        df[status_col] = df.apply(validate_language, axis=1)
    return df

def relocate_incorrect_text(df, language_prefixes, table_set_type):
    for lang in language_prefixes:
        lang = lang.lower()
        status_col = f"language_status_{lang.lower()}"
        title_col = f"{table_set_type.lower()}_title_{lang.lower()}"
        desc_col = f"{table_set_type.lower()}_description_{lang.lower()}"
        for idx, row in df.iterrows():
            status = row[status_col]
            if status and status.startswith("incorrect_new_"):
                target_lang = status.replace("incorrect_new_", "")
                new_title_col = f"{table_set_type.lower()}_title_{target_lang}"
                new_desc_col = f"{table_set_type.lower()}_description_{target_lang}"
                if pd.isna(row.get(new_title_col)) or not str(row.get(new_title_col)).strip():
                    df.at[idx, new_title_col] = row[title_col]
                    df.at[idx, new_desc_col] = row[desc_col]
                df.at[idx, title_col] = None
                df.at[idx, desc_col] = None
    return df

def add_language_quality(df):
    df["language_quality"] = None
    return df

def set_language_quality(df, language_prefixes):
    def determine_quality(row):
        current_quality = row.get("language_quality", None)
        if current_quality == "identical_description":
            return current_quality
        for lang in language_prefixes:
            status = row.get(f"language_status_{lang.lower()}", "")
            if status and status.startswith("incorrect"):
                return "incorrect"
        return "correct"
    df["language_quality"] = df.apply(determine_quality, axis=1)
    return df

def check_identical_descriptions(df, prefixes, table_set_type):
    desc_cols = [f"{table_set_type.lower()}_description_{lang.lower()}" for lang in prefixes]
    def has_identical_descriptions(row):
        descriptions = [str(row[col]).strip().lower() for col in desc_cols if pd.notnull(row[col]) and str(row[col]).strip()]
        seen = {}
        for desc in descriptions:
            if desc in seen:
                return True
            seen[desc] = True
        return False
    df["language_quality"] = df.apply(
        lambda row: "identical_description" if has_identical_descriptions(row) else row.get("language_quality", None),
        axis=1
    )
    return df

def fetch_dataset_metadata(config_path, db_name, table, identifier, base_cols, prefixes, table_type):
    config = load_config(config_path)
    config["dbname"] = db_name
    columns_to_load = generate_columns(identifier, base_cols, prefixes, table_type)
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        query = f"SELECT {', '.join(columns_to_load)} FROM {table}"
        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=colnames)
        cur.close()
        conn.close()
    except Exception as e:
        print("Error loading data:", e)
        df = pd.DataFrame(columns=columns_to_load)
    return df

def update_language_status_to_db(df, config_path, db_name, table_name, language_prefixes, table_set_type):
    import traceback
    config = load_config(config_path)
    config["dbname"] = db_name
    column_map = {
        f"language_status_{lang.lower()}": f"{table_set_type.lower()}_language_status_{lang.lower()}"
        for lang in language_prefixes
    }
    column_map["language_quality"] = f"{table_set_type.lower()}_language_quality"
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        for _, row in df.iterrows():
            values = [row.get(df_col) for df_col in column_map.keys()]
            set_clause = ", ".join([f"{db_col} = %s" for db_col in column_map.values()])
            query = f"UPDATE {table_name} SET {set_clause} WHERE dataset_identifier = %s"
            try:
                cur.execute(query, values + [row["dataset_identifier"]])
            except Exception:
                print("Fehler beim Schreiben eines Eintrags:")
                print("Query:", query)
                print("Werte:", values + [row["dataset_identifier"]])
                traceback.print_exc()
        conn.commit()
        cur.close()
        conn.close()
        print("Update erfolgreich abgeschlossen.")
    except Exception as e:
        print("Verbindungsfehler oder globaler Fehler:", e)
        traceback.print_exc()

def update_corrected_texts_to_db(df, config_path, db_name, table_name, language_prefixes, table_set_type):
    import traceback
    config = load_config(config_path)
    config["dbname"] = db_name

    text_columns = []
    for lang in language_prefixes:
        lang = lang.lower()
        text_columns.extend([
            f"{table_set_type.lower()}_title_{lang}",
            f"{table_set_type.lower()}_description_{lang}"
        ])

    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Update texts"):
            # 👉 Skip rows with correct language quality
            if row.get("language_quality") == "correct":
                continue

            values = [row.get(col) for col in text_columns]
            set_clause = ", ".join([f"{col} = %s" for col in text_columns])
            query = f"UPDATE {table_name} SET {set_clause} WHERE dataset_identifier = %s"
            try:
                cur.execute(query, values + [row["dataset_identifier"]])
            except Exception:
                print("❌ Fehler beim Schreiben von Textfeldern:")
                print("Query:", query)
                print("Werte:", values + [row["dataset_identifier"]])
                traceback.print_exc()
                conn.rollback()
                # continue
        conn.commit()
        cur.close()
        conn.close()
        print("Texte erfolgreich aktualisiert (nur nicht-'correct').")
    except Exception:
        print("❌ Verbindungsfehler oder globaler Fehler beim Text-Update:")
        traceback.print_exc()


def language_correction(config_file, dbname, table_name, language_prefixes, base_columns, table_set_type, min_length_lang_detect):
    df = fetch_dataset_metadata(
        config_path=config_file,
        db_name=dbname,
        table=table_name,
        identifier="dataset_identifier",
        base_cols=base_columns,
        prefixes=language_prefixes,
        table_type=table_set_type
    )
    if df.empty:
        print("No data loaded for processing.")
        return None

    transform_steps = [
        ("Process language status", lambda df: process_language_status(df, language_prefixes, table_set_type)),
        ("Add language quality column", add_language_quality),
        ("Check for identical descriptions", lambda df: check_identical_descriptions(df, language_prefixes, table_set_type)),
        ("Evaluate text lengths and detect language", lambda df: evaluate_text_lengths(df, language_prefixes, min_length_lang_detect, table_set_type)),
        ("Relocate incorrect texts", lambda df: relocate_incorrect_text(df, language_prefixes, table_set_type)),
        ("Set overall language quality", lambda df: set_language_quality(df, language_prefixes))
    ]

    for label, func in tqdm(transform_steps, desc="Processing transformations"):
        print(f"{label}")
        df = func(df)

    update_language_status_to_db(df, config_file, dbname, table_name, language_prefixes, table_set_type)
    update_corrected_texts_to_db(df, config_file, dbname, table_name, language_prefixes, table_set_type)

    return df
