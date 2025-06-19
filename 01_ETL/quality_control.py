import psycopg2
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from error_logger import log_error
import json

# --- load config ---
def load_config(config_path):
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_error(f"Failed to load configuration file: {e}", level="error")
        return None

# --- Load data for format mapping ---
def fetch_data_qc(config, limit=100):
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        query = """
        SELECT
            ds.dataset_identifier,
            dist.distribution_identifier,
            dist.distribution_format,
            dist.distribution_download_url,
            dist.distribution_access_url
        FROM merged_dataset_metadata ds
        LEFT JOIN merged_distribution_metadata dist
            ON ds.dataset_identifier = dist.dataset_identifier
        WHERE
            dist.distribution_format IS NOT NULL
            AND TRIM(dist.distribution_format) <> ''
            AND (
                dist.distribution_format_name IS NULL
                OR TRIM(dist.distribution_format_name) = ''
            )
        """
        if limit is not None:
            query += f" LIMIT {int(limit)}"

        cur.execute(query)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=colnames)

        cur.close()
        conn.close()
        if not df.empty:
            print("Successfully loaded data out of the db")
        return df

    except Exception as e:
        log_error(f"Error loading dataset metadata with distribution formats: {e}", level="error")
        return pd.DataFrame()

# --- Set description lengths ---
def set_description_length(config_file, dbname, language_prefixes, table_set_types, limit=None):
    config = load_config(config_file)
    if config is None:
        log_error("Configuration could not be loaded in set_description_length.")
        return

    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=dbname,
            user=config["user"],
            password=config["password"]
        )
        cursor = conn.cursor()

        for table_type in table_set_types:
            if table_type.strip().lower() == "dataset":
                table_name = "merged_dataset_metadata"
                base_column = "dataset_description_{}"
                new_column = "dataset_description_length_{}"
                pk_col = "dataset_identifier"
            elif table_type.strip().lower() == "distribution":
                table_name = "merged_distribution_metadata"
                base_column = "distribution_description_{}"
                new_column = "distribution_description_length_{}"
                pk_col = "distribution_identifier"
            else:
                log_error(f"Unknown table set type: {table_type}", level="error")
                continue

            for lang in language_prefixes:
                source_col = base_column.format(lang.lower())
                target_col = new_column.format(lang.lower())

                cursor.execute(f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_name='{table_name}' AND column_name='{target_col}'
                        ) THEN
                            ALTER TABLE {table_name}
                            ADD COLUMN {target_col} INTEGER;
                        END IF;
                    END
                    $$;
                """)

                if limit is None:
                    update_query = f"""
                        UPDATE {table_name}
                        SET {target_col} = CHAR_LENGTH({source_col})
                        WHERE {target_col} IS NULL AND {source_col} IS NOT NULL
                    """
                else:
                    update_query = f"""
                        WITH rows_to_update AS (
                            SELECT {pk_col}, CHAR_LENGTH({source_col}) AS new_length
                            FROM {table_name}
                            WHERE {target_col} IS NULL AND {source_col} IS NOT NULL
                            LIMIT {limit}
                        )
                        UPDATE {table_name} AS t
                        SET {target_col} = r.new_length
                        FROM rows_to_update r
                        WHERE t.{pk_col} = r.{pk_col}
                    """

                cursor.execute(update_query)
                print(f"Updated '{target_col}' in '{table_name}' where it was NULL.")

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        log_error(f"Database error in set_description_length: {e}", level="error")

# --- Set keyword counter ---
def set_keyword_count(config_file, dbname, language_prefixes, limit=None):
    config = load_config(config_file)
    if config is None:
        log_error("Configuration could not be loaded in set_keyword_count.")
        return

    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=dbname,
            user=config["user"],
            password=config["password"]
        )
        cursor = conn.cursor()

        table_name = "merged_dataset_metadata"
        pk_col = "dataset_identifier"
        base_column = "dataset_keyword_{}"
        new_column = "dataset_keyword_count_{}"

        for lang in language_prefixes:
            source_col = base_column.format(lang.lower())
            target_col = new_column.format(lang.lower())

            cursor.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name='{table_name}' AND column_name='{target_col}'
                    ) THEN
                        ALTER TABLE {table_name}
                        ADD COLUMN {target_col} INTEGER;
                    END IF;
                END
                $$;
            """)

            if limit is None:
                update_query = f"""
                    UPDATE {table_name}
                    SET {target_col} = CASE
                        WHEN {source_col} IS NOT NULL AND {source_col} <> '' THEN
                            array_length(string_to_array(trim(both '{{}}' from {source_col}), ','), 1)
                        ELSE 0
                    END
                    WHERE {target_col} IS NULL
                """
            else:
                update_query = f"""
                    WITH rows_to_update AS (
                        SELECT {pk_col},
                            CASE
                                WHEN {source_col} IS NOT NULL AND {source_col} <> '' THEN
                                    array_length(string_to_array(trim(both '{{}}' from {source_col}), ','), 1)
                                ELSE 0
                            END AS new_count
                        FROM {table_name}
                        WHERE {target_col} IS NULL
                        LIMIT {limit}
                    )
                    UPDATE {table_name} AS t
                    SET {target_col} = r.new_count
                    FROM rows_to_update r
                    WHERE t.{pk_col} = r.{pk_col}
                """

            cursor.execute(update_query)
            print(f"Updated '{target_col}' in '{table_name}' with keyword counts.")

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        log_error(f"Database error in set_keyword_count: {e}", level="error")

# --- Set format information ---
import pandas as pd
import psycopg2
from error_logger import log_error


def load_format_lookup(format_lockup_path):
    try:
        lookup_df = pd.read_csv(format_lockup_path)
        lookup_df["original_name_clean"] = lookup_df["original_name"].astype(str).str.strip().str.lower()
        return lookup_df
    except Exception as e:
        log_error(f"Could not load format lookup: {e}", level="error")
        return None


def normalize_distribution_format(df):
    df["distribution_format"] = df["distribution_format"].astype(str).str.strip()
    df["distribution_format"] = df["distribution_format"].replace("", "no_information")
    df["distribution_format"] = df["distribution_format"].fillna("no_information")
    df["distribution_format_clean"] = df["distribution_format"].str.lower()
    return df


def enrich_with_format_lookup(df, lookup_df):
    df_merged = df.merge(
        lookup_df,
        how="left",
        left_on="distribution_format_clean",
        right_on="original_name_clean"
    ).drop(columns=["original_name", "distribution_format_clean", "original_name_clean"])
    return df_merged


def calculate_format_counts(df_merged):
    df_valid = df_merged[df_merged["distribution_format"] != "no_information"]
    format_counts = (
        df_valid.groupby("dataset_identifier")["distribution_format"]
        .nunique()
        .reset_index(name="format_count")
    )
    df_with_counts = df_merged.merge(format_counts, on="dataset_identifier", how="left")
    df_with_counts["format_count"] = df_with_counts["format_count"].fillna(0).astype(int)
    return df_with_counts


def update_distribution_format_data(df_with_counts, db_config):
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        for _, row in df_with_counts.iterrows():
            if pd.notna(row["distribution_identifier"]):
                query = """
                    UPDATE merged_distribution_metadata
                    SET
                        distribution_format_name = %s,
                        distribution_format_type = %s,
                        distribution_format_cluster = %s,
                        distribution_format_geodata = %s
                    WHERE distribution_identifier = %s
                """

                params = tuple(None if pd.isna(p) else p for p in (
                    row["format_name"],
                    row["format_type"],
                    row["format_cluster"],
                    row["geodata_format"],
                    row["distribution_identifier"]
                ))
                cursor.execute(query, params)


        conn.commit()
        cursor.close()
        conn.close()
        print("Format information successfully updated.")
    except Exception as e:
        log_error(f"Database error in update_distribution_format_data: {e}", level="error")

def update_dataset_format_count(df_with_counts, db_config):
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        print("Updating dataset_distribution_format_count")

        format_counts = (
            df_with_counts[["dataset_identifier", "format_count"]]
            .drop_duplicates()
            .dropna(subset=["dataset_identifier"])
        )

        for _, row in format_counts.iterrows():
            query = """
                UPDATE merged_dataset_metadata
                SET dataset_distribution_format_count = %s
                WHERE dataset_identifier = %s
            """
            params = (int(row["format_count"]), row["dataset_identifier"])
            cursor.execute(query, params)

        conn.commit()
        cursor.close()
        conn.close()
        print("Dataset format counts updated.")
    except Exception as e:
        log_error(f"Database error in update_dataset_format_count: {e}", level="error")

def set_format_information(config_file, dbname, format_lockup, limit=None):
    config = load_config(config_file)
    if config is None:
        log_error("Configuration could not be loaded in set_format_information.", level="error")
        return

    db_config = {
        "host": config["host"],
        "port": config["port"],
        "database": dbname,
        "user": config["user"],
        "password": config["password"]
    }

    lookup_df = load_format_lookup(format_lockup)
    if lookup_df is None:
        return

    df = fetch_data_qc(db_config, limit=limit)
    if df.empty:
        log_error("No data fetched for format enrichment.")
        return

    df = normalize_distribution_format(df)
    df_merged = enrich_with_format_lookup(df, lookup_df)
    df_with_counts = calculate_format_counts(df_merged)

    update_distribution_format_data(df_with_counts, db_config)
    update_dataset_format_count(df_with_counts, db_config)

def fetch_data_for_url_check(config, limit=None):
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        query = """
        SELECT
            dist.distribution_identifier,
            dist.distribution_access_url,
            dist.distribution_download_url
        FROM merged_distribution_metadata dist
        WHERE
            dist.distribution_identifier IS NOT NULL
            AND (
                dist.distribution_access_url IS NOT NULL
                OR dist.distribution_download_url IS NOT NULL
            )
            AND (
                dist.distribution_access_url_status_code IS NULL
                OR dist.distribution_download_url_status_code IS NULL
            )
        """
        if limit is not None:
            query += f" LIMIT {int(limit)}"

        cur.execute(query)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=colnames)

        cur.close()
        conn.close()

        if not df.empty:
            print(f"Loaded {len(df)} records from the DB.")
        return df

    except Exception as e:
        log_error(f"Error fetching data for URL check: {e}", level="error")
        return pd.DataFrame()

def check_url_status(url):
    MAX_WORKERS = 30
    TIMEOUT = 5

    if not url or url.strip() == "":
        return url, None
    try:
        response = requests.head(url, allow_redirects=True, timeout=TIMEOUT)
        return url, response.status_code
    except requests.exceptions.RequestException:
        return url, None
    

def update_status_codes_in_db(df, db_config):
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        for _, row in df.iterrows():
            access_status = row["access_url_status_code"]
            download_status = row["download_url_status_code"]

            access_status = 0 if pd.isna(access_status) else int(access_status)
            download_status = 0 if pd.isna(download_status) else int(download_status)

            cur.execute("""
                UPDATE merged_distribution_metadata
                SET
                    distribution_access_url_status_code = %s,
                    distribution_download_url_status_code = %s
                WHERE distribution_identifier = %s
            """, (
                access_status,
                download_status,
                row["distribution_identifier"]
            ))

        conn.commit()
        cur.close()
        conn.close()
        print("Status codes successfully updated in DB.")
    except Exception as e:
        log_error(f"Database update failed: {e}", level="error")


def set_distribution_url_status_codes(config_path, dbname, limit, max_workers=30, timeout=5):
    config = load_config(config_path)
    if config is None:
        log_error("Configuration could not be loaded in set_distribution_url_status_codes.", level="error")
        return

    db_config = {
        "host": config["host"],
        "port": config["port"],
        "database": dbname,
        "user": config["user"],
        "password": config["password"]
    }

    df = fetch_data_for_url_check(db_config,limit=limit)
    if df.empty:
        print("No data to check.")
        return

    all_urls = pd.concat([
        df["distribution_access_url"].dropna().astype(str).str.strip(),
        df["distribution_download_url"].dropna().astype(str).str.strip()
    ]).unique()

    print(f"Checking {len(all_urls)} unique URLs...")
    url_status_map = {}

    def check_url_status(url):
        if not url or url.strip() == "":
            return url, None
        try:
            response = requests.head(url, allow_redirects=True, timeout=timeout)
            return url, response.status_code
        except requests.exceptions.RequestException:
            return url, None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(check_url_status, url): url for url in all_urls}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Checking URLs"):
            url, status = future.result()
            url_status_map[url] = status

    # Statuscodes zuordnen
    df["access_url_status_code"] = df["distribution_access_url"].astype(str).str.strip().map(url_status_map)
    df["download_url_status_code"] = df["distribution_download_url"].astype(str).str.strip().map(url_status_map)

    update_status_codes_in_db(df, db_config)




# --- Hauptfunktion ---
def set_quality_indicators(
    config_file=r"01_ETL\21_load\db_config.json",
    dbname="4M",
    language_prefixes=["DE", "EN", "FR", "IT"],
    table_set_types=["dataset", "distribution"],
    format_lockup=r"04_QC\formats_lockup.csv",
    limit=None
):
    


    config = load_config(config_file)
    if config is None:
        log_error("No config, stop quality control script", level="error")
        return

    set_description_length(config_file, dbname, language_prefixes, table_set_types, limit)
    set_keyword_count(config_file, dbname, language_prefixes, limit)
    set_format_information(config_file, dbname, format_lockup, limit)

    set_distribution_url_status_codes(
    config_file,
    dbname,
    limit,
    max_workers=30,
    timeout=5
)


# --- Aufruf ---
if __name__ == "__main__":
    result = set_quality_indicators()
    print(result)
