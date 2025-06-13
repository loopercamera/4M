import psycopg2
import pandas as pd
import json
import ast
import re
import numpy as np
import os
from nltk.corpus import stopwords
from nltk import download
from sentence_transformers import SentenceTransformer
from sklearn.cluster import DBSCAN
from sklearn.neighbors import NearestNeighbors
from kneed import KneeLocator
import umap.umap_ as umap

# Required NLTK stopwords for selected languages
nltk_languages = {
    "de": "german",
    "en": "english",
    "fr": "french",
    "it": "italian"
}

# Download stopwords
download('stopwords')

# store stopwords in a dictionary
stopword_dict = {}
for code, nltk_name in nltk_languages.items():
    try:
        stopword_dict[code] = set(stopwords.words(nltk_name))
    except Exception as e:
        print(f"Could not load stopwords for {code}: {e}")
        stopword_dict[code] = set()

# Load SBERT model
model = SentenceTransformer('all-MiniLM-L6-v2')

def load_config(config_path):
    with open(config_path) as f:
        return json.load(f)

def generate_columns(identifier, base_cols, prefixes, extra_columns=None):
    """Generate dynamic column names based on language prefixes."""
    language_columns = [f"{col}_{lang.lower()}" for lang in prefixes for col in base_cols]
    columns = [identifier] + language_columns
    if extra_columns:
        columns += extra_columns
    return columns

def fetch_data(config, db_name, table, columns, limit=None):
    """Fetch data from a PostgreSQL table."""
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

def update_data(df, identifier_column, value_column, new_value):
    """Update a specific column in the DataFrame with new values."""
    df.loc[:, value_column] = new_value
    return df

def write_back_to_db(config, db_name, table, df, identifier_column, batch_size=1000):
    """Write updated data back to the database in batches."""
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
        print("Error writing data back:", e)

# Set language preferences and processing configuration
languages = ["de", "en", "fr", "it", "rm", "unknown"]

def find_keywords_and_language(row):
    """
    Return the first non-empty keyword list from preferred languages,
    along with its corresponding language code.
    """
    if isinstance(row['dataset_language'], list):
        for lang in languages:
            if lang.upper() in row['dataset_language']:
                col = f"dataset_keyword_{lang}"
                if col in row and isinstance(row[col], list) and any(isinstance(kw, str) and kw.strip() for kw in row[col]):
                    return row[col], lang

    for lang in languages:
        col = f"dataset_keyword_{lang}"
        if col in row and isinstance(row[col], list) and any(isinstance(kw, str) and kw.strip() for kw in row[col]):
            return row[col], lang

    return None, "unknown"

def remove_keywords(keyword_list):
    """Remove common or irrelevant keywords from a list."""
    keywords_to_remove = [
        'verkehr', 'opendata', 'geoportal', 'opendata.swiss',
        'bgdi bundesgeodaten-infrastruktur', 'bgdi-bundesgeodaten-infrastruktur',
        'geodaten', 'geobasisdaten', 'geodatenmodell'
    ]
    if isinstance(keyword_list, list):
        return [kw for kw in keyword_list if kw.lower() not in keywords_to_remove]
    return keyword_list

def tokenize(text, lang_code):
    """
    Tokenize text and remove stopwords based on the given language code.
    """
    tokens = re.findall(r'\b\w+\b', text.lower())
    stopwords_set = stopword_dict.get(lang_code.lower(), set())
    return [token for token in tokens if token not in stopwords_set and len(token) > 2]


# === MAIN EXECUTION ===

# Load config and database settings
db_name = "4M"
table = "merged_dataset_metadata"
identifier = "dataset_identifier"
base_cols = ["dataset_keyword", "dataset_description", "dataset_title"]
prefixes = ["DE", "EN", "FR", "IT", "RM", "UNKNOWN"]
additional = ["dataset_is_mobility", "dataset_cluster_id", "dataset_language"]

script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, "data", "db_config.json")
config = load_config(config_path)

columns = generate_columns(identifier, base_cols, prefixes, extra_columns=additional)

# Load data
df = fetch_data(config, db_name, table, columns)

# Filter to only rows labeled as mobility
df = df[df['dataset_is_mobility'] == True]
print(f"Filtered mobility entries: {len(df)}")

# Fix datatype of language and keyword columns
df['dataset_language'] = df['dataset_language'].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith('[') else x
)

keyword_columns = [f"dataset_keyword_{lang}" for lang in ["de", "en", "fr", "it", "rm", "unknown"]]
for col in keyword_columns:
    if col in df.columns:
        df[col] = df[col].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith('[') else x
        )

# Select and clean keywords
# Extract both keywords and source language
df[['keywords_selected', 'keywords_lang']] = df.apply(
    lambda row: pd.Series(find_keywords_and_language(row)),
    axis=1
)
df['keywords_selected'] = df['keywords_selected'].apply(remove_keywords)

# Tokenization
valid_rows = df['keywords_selected'].apply(lambda x: isinstance(x, list) and len(x) > 0)
texts = []
for _, row in df.loc[valid_rows].iterrows():
    lang = row['keywords_lang']
    kw_list = row['keywords_selected']
    tokenized = tokenize(" ".join(kw_list), lang)
    texts.append(tokenized)

documents = [" ".join(tokens) for tokens in texts]

# Generate SBERT embeddings
print("Generating SBERT embeddings...")
embeddings = model.encode(documents, show_progress_bar=True)

# UMAP dimensionality reduction
print("Reducing dimensionality with UMAP...")
reducer = umap.UMAP(n_components=50, random_state=1)
reduced_embeddings = reducer.fit_transform(embeddings)

# Find optimal epsilon value using k-distance and KneeLocator
k = 4
neigh = NearestNeighbors(n_neighbors=k)
neigh.fit(reduced_embeddings)
distances, _ = neigh.kneighbors(reduced_embeddings)
k_distances = np.sort(distances[:, -1])

kneedle = KneeLocator(range(len(k_distances)), k_distances, curve='convex', direction='increasing')
optimal_eps = kneedle.knee_y
print(f"Automatically determined optimal eps: {optimal_eps:.4f}")

# Apply DBSCAN clustering
clustering = DBSCAN(eps=optimal_eps, min_samples=k)
labels = clustering.fit_predict(reduced_embeddings)

# Assign cluster labels to DataFrame
df.loc[valid_rows, 'dataset_cluster_id'] = labels

# Statistics
cluster_distribution = df['dataset_cluster_id'].value_counts().sort_index()
num_noise = cluster_distribution.get(-1, 0)
num_clustered = cluster_distribution[cluster_distribution.index >= 0].sum()

print("Cluster statistics:")
print(f"- Noise entries (-1): {num_noise}")
print(f"- Clustered entries (>=0): {num_clustered}")
print(f"- Number of clusters: {len(set(labels) - {-1})}")

# Update the DataFrame with new cluster IDs
df = update_data(df, identifier_column=identifier, value_column='dataset_cluster_id', new_value=df['dataset_cluster_id'])

# Drop any temporary or derived columns before writing back to DB
df = df.drop(columns=['keywords_selected', 'keywords_lang'], errors='ignore')

# Write results back to the database
write_back_to_db(config, db_name, table, df, identifier_column=identifier)

print("Script completed successfully.")