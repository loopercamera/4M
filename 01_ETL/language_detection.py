import psycopg2
import psycopg2.extras
import pandas as pd
import json
from langdetect import detect_langs
from error_logger import log_error  # Import log_error from error_logger.py

# --- CONFIGURABLE (CAN BE OVERRIDDEN VIA FUNCTION CALL) ---
def load_config(config_path):
    with open(config_path) as f:
        return json.load(f)

def detect_preferred_language(text, language_prefixes):
    try:
        langs = detect_langs(text)
        for lang in langs[:2]:
            if lang.lang in language_prefixes:
                return [[lang.lang], lang.prob]
        return [['not_found'], 0.0]
    except:
        return [['not_found'], 0.0]

def build_column_map(columns_to_map, unknown_suffix, lang):
    return {
        f"{col}{unknown_suffix}": f"{col}_{lang}" for col in columns_to_map
    }

def process_language_mapping(
    config_path,
    dbname,
    table_name,
    language_col,
    identifier_col,
    columns_to_map,
    unknown_suffix="_unknown",
    language_prefixes=["de", "en", "fr", "it"],
    table_set_type="dataset"
):
    log_error(f"Process language mapping started fot the table: {table_name}", "info")
    config = load_config(config_path)
    config["dbname"] = dbname

    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        unknown_columns = [f"{col}{unknown_suffix}" for col in columns_to_map]
        target_columns = [f"{col}_{lang}" for col in columns_to_map for lang in language_prefixes]
        not_null_filter = f"{unknown_columns[0]} IS NOT NULL"

        query = f"""
            SELECT {identifier_col}, {language_col}, {', '.join(unknown_columns)}
            FROM {table_name}
            WHERE {not_null_filter}
        """

        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=colnames)
        cur.close()
        conn.close()
    except Exception as e:
        log_error(f"Error loading data:", "error", e)
        return

    if df.empty:
        log_error(f"No data loaded for processing.", "error")
        return

    # Determine language: use existing if available, otherwise detect
    df['detected_lang'] = df[language_col].fillna('').apply(lambda x: x.lower() if x.lower() in language_prefixes else None)

    column_to_detect = unknown_columns[0]
    mask = df['detected_lang'].isnull()
    detected = df.loc[mask, column_to_detect].apply(lambda x: pd.Series(detect_preferred_language(x, language_prefixes)))
    detected.columns = ['detected_language', 'language_confidence']
    df.loc[mask, ['detected_language', 'language_confidence']] = detected

    df['detected_lang'] = df['detected_lang'].fillna(
        df['detected_language'].apply(lambda x: x[0].lower() if isinstance(x, list) and len(x) > 0 else 'not_found')
    )

    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        for _, row in df.iterrows():
            lang = row['detected_lang']
            if lang not in language_prefixes:
                continue

            column_map = build_column_map(columns_to_map, unknown_suffix, lang)
            updates = []
            values = []

            for unknown_col, target_col in column_map.items():
                if pd.notna(row.get(unknown_col)):
                    updates.append(f"{target_col} = %s")
                    values.append(row[unknown_col])
                    updates.append(f"{unknown_col} = NULL")

            if not updates:
                continue

            updates.append(f"{language_col} = %s")
            values.append(lang)
            values.append(row[identifier_col])

            update_query = f"""
                UPDATE {table_name}
                SET {', '.join(updates)}
                WHERE {identifier_col} = %s
            """
            cur.execute(update_query, values)

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        log_error(f"Error updating metadata:", "error", e)


    log_error(f"Detect languages based on content in title/description fields: {table_name}", "info")

    try:
        config = load_config(config_path)
        config["dbname"] = dbname
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        # STEP 1: Detect languages based on content in title/description fields
        update_languages_query = f"""
            UPDATE {table_name}
            SET {language_col} = TRIM(BOTH ',' FROM
                CONCAT_WS(',',
                    CASE WHEN (
                            COALESCE({table_set_type}_title_DE, '') <> '' OR
                            COALESCE({table_set_type}_description_DE, '') <> ''
                        )
                        THEN 'de' ELSE NULL END,
                    CASE WHEN (
                            COALESCE({table_set_type}_title_EN, '') <> '' OR
                            COALESCE({table_set_type}_description_EN, '') <> ''
                        )
                        THEN 'en' ELSE NULL END,
                    CASE WHEN (
                            COALESCE({table_set_type}_title_FR, '') <> '' OR
                            COALESCE({table_set_type}_description_FR, '') <> ''
                        )
                        THEN 'fr' ELSE NULL END,
                    CASE WHEN (
                            COALESCE({table_set_type}_title_IT, '') <> '' OR
                            COALESCE({table_set_type}_description_IT, '') <> ''
                        )
                        THEN 'it' ELSE NULL END
                )
            );
        """
        cur.execute(update_languages_query)
        conn.commit()
        # STEP 2: Wrap language list in brackets ['de','en']
        wrap_language_query = f"""
            WITH exploded AS (
                SELECT ctid AS row_id,
                    string_agg(QUOTE_LITERAL(trim(value)), ',') AS lang_list
                FROM (
                    SELECT ctid,
                        unnest(string_to_array({language_col}, ',')) AS value
                    FROM {table_name}
                    WHERE {language_col} IS NOT NULL
                ) sub
                GROUP BY ctid
            )
            UPDATE {table_name} AS m
            SET {language_col} = '[' || e.lang_list || ']'
            FROM exploded e
            WHERE m.ctid = e.row_id;
        """
        cur.execute(wrap_language_query)
        conn.commit()
        cur.close()
        conn.close()
        log_error(f"Metadata mapped and cleaned in: {table_name}", "info")
    except Exception as e:
        print("Error cleaning language column:", e)







