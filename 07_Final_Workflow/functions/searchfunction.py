import os
import json
import psycopg2
import pandas as pd
from rank_bm25 import BM25Okapi
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
from tabulate import tabulate
from functions.hexmap_plot import plot_hex_map

# === Initial Setup ===
nltk.download('punkt')
nltk.download('stopwords')

german_stopwords = set(stopwords.words('german'))
stemmer = SnowballStemmer("german")
languages = ["de", "en", "fr", "it", "rm", "unknown"]

def preprocess(text):
    tokens = word_tokenize(str(text).lower())
    return [stemmer.stem(w) for w in tokens if w.isalnum() and w not in german_stopwords]

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
        print("Error loading data:", e)
        return pd.DataFrame(columns=columns)

def determine_language_and_keywords(row):
    for lang in languages:
        col = f"dataset_keyword_{lang}"
        val = row.get(col, "")
        if isinstance(val, str) and val.startswith("{") and val.endswith("}"):
            parsed = [v.strip() for v in val.strip("{}").split(",") if v.strip()]
        elif isinstance(val, (list, set)):
            parsed = list(val)
        else:
            parsed = []
        if parsed:
            return pd.Series([lang, parsed])
    return pd.Series(["unknown", []])

def resolve_region(row):
    canton = str(row.get("dataset_location_canton", "")).strip().lower()
    country = str(row.get("dataset_location_country", "")).strip().lower()
    if canton and canton not in ["none", "nan", "not_found"]:
        return canton.upper()
    elif country and country not in ["none", "nan", "not_found"]:
        return country.upper()
    else:
        return "None"

def dbscan_search(query, df, alpha=0.7, beta=0.3):
    df = df[['final_title', 'final_description', 'final_keywords', 'dataset_cluster_id',
             "dataset_location", "dataset_location_id", "dataset_location_canton", "dataset_location_country",
             'combined_text']].fillna('')

    tokenized_docs = df['combined_text'].apply(preprocess).tolist()
    bm25_docs = BM25Okapi(tokenized_docs)
    query_tokens = preprocess(query)
    doc_scores = bm25_docs.get_scores(query_tokens)

    valid_cluster_df = df[~df['dataset_cluster_id'].isin([-1, -2])]
    cluster_representations = {}
    for cluster_id in valid_cluster_df['dataset_cluster_id'].unique():
        cluster_texts = valid_cluster_df[valid_cluster_df['dataset_cluster_id'] == cluster_id]['combined_text']
        tokens = [token for doc in cluster_texts for token in preprocess(doc)]
        cluster_representations[cluster_id] = tokens

    if cluster_representations:
        bm25_clusters = BM25Okapi(list(cluster_representations.values()))
        cluster_scores = bm25_clusters.get_scores(query_tokens)
        cluster_score_map = dict(zip(cluster_representations.keys(), cluster_scores))
    else:
        cluster_score_map = {}

    df['bm25_score'] = doc_scores
    df['cluster_score'] = df['dataset_cluster_id'].apply(lambda cid: cluster_score_map.get(cid, 0))
    df['combined_score'] = alpha * df['bm25_score'] + beta * df['cluster_score']

    if cluster_score_map:
        best_cluster_id, best_cluster_score = max(cluster_score_map.items(), key=lambda x: x[1])
    else:
        best_cluster_id, best_cluster_score = None, 0

    if best_cluster_id is not None and best_cluster_score >= 1:
        df_cluster = df[df['dataset_cluster_id'] == best_cluster_id]
        df_bm25_only = df[(df['bm25_score'] > 0) & (df['dataset_cluster_id'] != best_cluster_id)]
        df_result = pd.concat([df_cluster, df_bm25_only])
    else:
        df_result = df[df['bm25_score'] > 0]

    df_result = df_result[df_result['combined_score'] > 0]
    df_result = df_result.sort_values(by='combined_score', ascending=False).reset_index(drop=True)

    print(tabulate(
        df_result[['final_title', 'final_description', 'final_keywords', "dataset_location", "dataset_location_id",
                   "dataset_location_canton", "dataset_location_country", 'bm25_score', 'cluster_score', 'combined_score']],
        headers=['Title', 'Description', 'Keywords', 'Location', 'Location ID', 'Canton', 'Country',
                 'BM25', 'ClusterScore', 'CombinedScore'],
        tablefmt='fancy_grid', showindex=True))

    df_result['region'] = df_result.apply(resolve_region, axis=1)
    region_counts = df_result['region'].value_counts().to_dict()

    if region_counts:
        plot_hex_map(
            region_counts,
            titel=f'Vorhandene Datensätze zum Thema "{query}"',
            untertitel='Schematische Darstellung der Verteilung der Suchergebnisse auf die Kantone oder Schweiz'
        )

    return df_result

def prepare_data():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "../data", "db_config.json")

    db_name = "4M"
    table = "merged_dataset_metadata"
    identifier = "dataset_identifier"
    base_cols = ["dataset_title", "dataset_keyword", "dataset_description"]
    prefixes = ["DE", "EN", "FR", "IT", "RM", "UNKNOWN"]
    ismobility = ["dataset_is_mobility", "dataset_cluster_id", "dataset_location", "dataset_location_id",
                  "dataset_location_district", "dataset_location_canton", "dataset_location_country"]

    config = load_config(config_path)
    columns = generate_columns(identifier, base_cols, prefixes, extra_columns=ismobility)
    df = fetch_data(config, db_name, table, columns)

    df = df[df['dataset_is_mobility'] == True]
    df[['lang_used', 'final_keywords']] = df.apply(determine_language_and_keywords, axis=1)
    df['final_title'] = df.apply(lambda row: row.get(f'dataset_title_{row["lang_used"]}', ""), axis=1)
    df['final_description'] = df.apply(lambda row: row.get(f'dataset_description_{row["lang_used"]}', ""), axis=1)
    df['final_keywords'] = df['final_keywords'].apply(lambda kws: ", ".join(kws) if isinstance(kws, list) else str(kws))
    df['combined_text'] = df['final_title'] + ' ' + df['final_description'] + ' ' + df['final_keywords']

    return df