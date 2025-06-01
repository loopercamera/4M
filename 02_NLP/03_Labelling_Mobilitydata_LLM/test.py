import psycopg2
import pandas as pd
import json

def load_config(config_path):
    with open(config_path) as f:
        return json.load(f)

def generate_columns(identifier, base_cols, prefixes, extra_columns=None):
    language_columns = [f"{col}_{lang.lower()}" for lang in prefixes for col in base_cols]
    columns = [identifier] + language_columns
    if extra_columns:
        columns += extra_columns
    return columns


def fetch_data(config, db_name, table, columns):
    config["dbname"] = db_name
    try:
        conn = psycopg2.connect(**config)
        df = pd.read_sql_query(f"SELECT {', '.join(columns)} FROM {table}", conn)
        conn.close()
        return df
    except Exception as e:
        print("Fehler beim Laden der Daten:", e)
        return pd.DataFrame(columns=columns)

def update_data(df, identifier_column, value_column, new_value):
    df.loc[:, value_column] = new_value
    return df

def write_back_to_db(config, db_name, table, df, identifier_column):
    config["dbname"] = db_name
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        for _, row in df.iterrows():
            cur.execute(
                f"UPDATE {table} SET {', '.join([f'{col} = %s' for col in df.columns if col != identifier_column])} "
                f"WHERE {identifier_column} = %s",
                [row[col] for col in df.columns if col != identifier_column] + [row[identifier_column]]
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print("Fehler beim Zurückschreiben der Daten:", e)

# Beispielnutzung
config_path = r"02_NLP/03_Labelling_Mobilitydata_LLM/data/db_config.json"
db_name = "4M_copy"
table = "merged_dataset_metadata"
identifier = "dataset_identifier"
base_cols = ["dataset_title", "dataset_keyword", "dataset_description"]
prefixes = ["DE", "EN", "FR", "IT"]
ismobility = ["dataset_is_mobility"]

config = load_config(config_path)
columns = generate_columns(identifier, base_cols, prefixes, extra_columns=ismobility)
df = fetch_data(config, db_name, table, columns)
print(df)