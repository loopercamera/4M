import psycopg2
import pandas as pd
import json
import numpy as np
from google import genai
from google.genai import types
from tqdm import tqdm
from itertools import cycle
import time
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(script_dir, "data", "apikeys.json")) as f:
    config_keys = json.load(f)
API_KEYS = config_keys["GOOGLE_API_KEYS"]
API_KEYS_CYCLE = cycle(API_KEYS)

def load_config(config_path):
    with open(config_path) as f:
        return json.load(f)

def generate_columns(identifier, base_cols, prefixes, extra_columns=None):
    language_columns = [f"{col}_{lang.lower()}" for lang in prefixes for col in base_cols]
    columns = [identifier] + language_columns
    if extra_columns:
        columns += extra_columns
    return columns

def fetch_data(config, db_name, table, columns, limit=None):
    config["dbname"] = db_name
    try:
        conn = psycopg2.connect(**config)
        sql = f"SELECT {', '.join(columns)} FROM {table}"
        if limit:
            sql += f" LIMIT {limit}"
        df = pd.read_sql_query(sql, conn)
        conn.close()
        return df
    except Exception as e:
        print("Error while loading data:", e)
        return pd.DataFrame(columns=columns)

def update_data(df, identifier_column, value_column, new_value):
    df.loc[:, value_column] = new_value
    return df

def write_back_to_db(config, db_name, table, df, identifier_column, batch_size=1000):
    config["dbname"] = db_name
    total_rows = len(df)
    try:
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch_df = df.iloc[start:end]
            conn = psycopg2.connect(**config)
            cur = conn.cursor()
            for _, row in batch_df.iterrows():
                cur.execute(
                    f"UPDATE {table} SET {', '.join([f'{col} = %s' for col in df.columns if col != identifier_column])} "
                    f"WHERE {identifier_column} = %s",
                    [row[col] for col in df.columns if col != identifier_column] + [row[identifier_column]]
                )
            conn.commit()
            cur.close()
            conn.close()
            print(f"Batch {start}–{end} updated successfully.")
    except Exception as e:
        print("Error while writing data back:", e)

def assign_iteration_index(df):
    df = df.copy()
    df['iteration_index'] = None
    def not_empty(col):
        return ~df[col].isna() & (df[col].astype(str).str.strip() != '')
    index_rules = [
        (1,  not_empty('dataset_description_de') & not_empty('dataset_title_de')),
        (2,  not_empty('dataset_description_de')),
        (3,  not_empty('dataset_description_en') & not_empty('dataset_title_en')),
        (4,  not_empty('dataset_description_en')),
        (5,  not_empty('dataset_description_fr') & not_empty('dataset_title_fr')),
        (6,  not_empty('dataset_description_fr')),
        (7,  not_empty('dataset_description_it') & not_empty('dataset_title_it')),
        (8,  not_empty('dataset_description_it')),
        (9,  not_empty('dataset_description_rm') & not_empty('dataset_title_rm')),
        (10, not_empty('dataset_description_rm')),
        (11, not_empty('dataset_title_de')),
        (12, not_empty('dataset_title_en')),
        (13, not_empty('dataset_title_fr')),
        (14, not_empty('dataset_title_it')),
        (15, not_empty('dataset_title_rm')),
        (16, not_empty('dataset_description_unknown') & not_empty('dataset_title_unknown')),
        (17, not_empty('dataset_description_unknown')),
        (18, not_empty('dataset_title_unknown')),
    ]
    for index, condition in index_rules:
        df.loc[df['iteration_index'].isna() & condition, 'iteration_index'] = index
    return df

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "data", "db_config.json")
    db_name = "4M"
    table = "merged_dataset_metadata"
    identifier = "dataset_identifier"
    base_cols = ["dataset_title", "dataset_keyword", "dataset_description"]
    prefixes = ["DE", "EN", "FR", "IT", "RM", "UNKNOWN"]
    ismobility = ["dataset_is_mobility"]

    config = load_config(config_path)
    columns = generate_columns(identifier, base_cols, prefixes, extra_columns=ismobility)
    df = fetch_data(config, db_name, table, columns)
    df = df[df['dataset_is_mobility'].isna()]
    print(f"Rows to process: {len(df)}")

    df = assign_iteration_index(df)

    key_count = len(API_KEYS)
    chunk_size = 1
    requests_per_key = 15
    current_key_index = 0
    key_request_counter = 0
    cycle_start_time = time.time()

    group_chunk_lines = {
    "1": lambda row: f"Titel: {row['dataset_title_de']}\nBeschreibung: {row['dataset_description_de']}",
    "2": lambda row: f"Beschreibung: {row['dataset_description_de']}",
    "3": lambda row: f"Titel: {row['dataset_title_en']}\nBeschreibung: {row['dataset_description_en']}",
    "4": lambda row: f"Beschreibung: {row['dataset_description_en']}",
    "5": lambda row: f"Titel: {row['dataset_title_fr']}\nBeschreibung: {row['dataset_description_fr']}",
    "6": lambda row: f"Beschreibung: {row['dataset_description_fr']}",
    "7": lambda row: f"Titel: {row['dataset_title_it']}\nBeschreibung: {row['dataset_description_it']}",
    "8": lambda row: f"Beschreibung: {row['dataset_description_it']}",
    "9": lambda row: f"Titel: {row['dataset_title_rm']}\nBeschreibung: {row['dataset_description_rm']}",
    "10": lambda row: f"Beschreibung: {row['dataset_description_rm']}",
    "11": lambda row: f"Titel: {row['dataset_title_de']}",
    "12": lambda row: f"Titel: {row['dataset_title_en']}",
    "13": lambda row: f"Titel: {row['dataset_title_fr']}",
    "14": lambda row: f"Titel: {row['dataset_title_it']}",
    "15": lambda row: f"Titel: {row['dataset_title_rm']}",
    "16": lambda row: f"Titel: {row['dataset_title_unknown']}\nBeschreibung: {row['dataset_description_unknown']}",
    "17": lambda row: f"Beschreibung: {row['dataset_description_unknown']}",
    "18": lambda row: f"Titel: {row['dataset_title_unknown']}",
}

    relevant_columns = [
        'dataset_title_de', 'dataset_description_de',
        'dataset_title_en', 'dataset_description_en',
        'dataset_title_fr', 'dataset_description_fr',
        'dataset_title_it', 'dataset_description_it',
        'dataset_title_rm', 'dataset_description_rm',
        'dataset_title_unknown', 'dataset_description_unknown'
    ]

    df['mobilitydata_labelled'] = None

    for group_name, group_df in df.groupby('iteration_index'):
        print(f"Processing group: {group_name} with {len(group_df)} entries")
        for i in tqdm(range(0, len(group_df), chunk_size)):
            if key_request_counter >= requests_per_key:
                current_key_index += 1
                key_request_counter = 0
                if current_key_index >= key_count:
                    elapsed = time.time() - cycle_start_time
                    if elapsed < 60:
                        time.sleep(61 - elapsed)
                    current_key_index = 0
                    cycle_start_time = time.time()
            CURRENT_API_KEY = API_KEYS[current_key_index]
            client = genai.Client(api_key=CURRENT_API_KEY)
            chunk_df = group_df.iloc[i:i + chunk_size][relevant_columns]
            formatter = group_chunk_lines[str(group_name)]
            chunk_lines = chunk_df.apply(formatter, axis=1).tolist()
            prompt = "Handelt es sich bei folgendem Inhalt um Verkehrs- oder Mobilitätsdaten?Antworte nur mit T (True) oder F (False).\n\n" + "\n\n".join(chunk_lines) + "Antwort:"

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = client.models.generate_content_stream(
                        model="gemini-2.0-flash-lite-001",
                        contents=[prompt],
                        config=types.GenerateContentConfig(max_output_tokens=chunk_size * 2, temperature=0)
                    )
                    result_text = "".join(chunk.text for chunk in response)
                    break
                except Exception as e:
                    if "RESOURCE_EXHAUSTED" in str(e) or "429" in str(e):
                        print(f"Rate limit reached. Waiting 60 seconds... (Attempt {attempt+1})")
                        time.sleep(60)
                    else:
                        print(f"Fehler: {e}")
                        break
            else:
                df.loc[chunk_df.index, 'mobilitydata_labelled'] = 'ERROR'
                continue

            predictions = result_text.strip().splitlines()
            if len(predictions) != chunk_size:
                df.loc[chunk_df.index, 'mobilitydata_labelled'] = 'ERROR'
                continue

            df.loc[chunk_df.index, 'mobilitydata_labelled'] = predictions
            key_request_counter += 1
            time.sleep(0.8)

    df['mobilitydata_labelled'] = df['mobilitydata_labelled'].map({'T': True, 'F': False})
    df = df[df['mobilitydata_labelled'].isin([True, False])].copy()
    df = update_data(df, identifier_column=identifier, value_column='dataset_is_mobility', new_value=df['mobilitydata_labelled'])
    df.drop(columns=['iteration_index', 'mobilitydata_labelled'], inplace=True)
    write_back_to_db(config, db_name, table, df, identifier_column=identifier)

if __name__ == "__main__":
    main()