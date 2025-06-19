"""
Combined Metadata Module

Provides utilities to:
- Load configuration
- Load metadata CSVs into PostgreSQL
- Reset/create/drop PostgreSQL databases
- Delete metadata entries by XML filenames (directly or via CSV)

Usage:
    python metadata_manager.py --folder ./data --config ./db_config.json --sql ./tables.sql
    python metadata_manager.py --csv removeorder_metadata_opendata.swiss.csv --db 4M
"""

import psycopg2
from psycopg2 import connect, sql, errors
import json
import os
import csv
import argparse

from functions.error_logger import log_error, log_start_message

# --------------- CONFIG ----------------
def load_config(config_path):
    """Load database configuration from a JSON file."""
    with open(config_path) as f:
        return json.load(f)

# --------------- DATABASE UTILITIES ----------------
def database_exists(db_name, config_path):
    config = load_config(config_path)
    conn = connect(database="postgres", user=config["user"], password=config["password"],
                   host=config["host"], port=config["port"])
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
        return cur.fetchone() is not None
    except Exception as e:
        log_error("Error checking if database exists", exception=e)
        return False
    finally:
        cur.close()
        conn.close()

def create_database(db_name, config_path, sql_file_path):
    config = load_config(config_path)
    conn = connect(database="postgres", user=config["user"], password=config["password"],
                   host=config["host"], port=config["port"])
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL("CREATE DATABASE {} ").format(sql.Identifier(db_name)))
        log_error(f"Database '{db_name}' created successfully", "info")
        execute_sql_script(db_name, sql_file_path, config_path)
    except errors.DuplicateDatabase:
        log_error(f"Database '{db_name}' already exists.", level="warning")
    except Exception as e:
        log_error("Error creating database", exception=e)
    finally:
        cur.close()
        conn.close()

def delete_database(db_name, config_path):
    config = load_config(config_path)
    conn = connect(database="postgres", user=config["user"], password=config["password"],
                   host=config["host"], port=config["port"])
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL("DROP DATABASE {} ").format(sql.Identifier(db_name)))
        log_error(f"Database '{db_name}' deleted.", "info")
    except errors.InvalidCatalogName:
        log_error(f"Database '{db_name}' does not exist.", level="warning")
    except errors.ObjectInUse:
        log_error(f"Cannot drop '{db_name}': it is currently being accessed.", level="error")
    except Exception as e:
        log_error("Error deleting database", exception=e)
    finally:
        cur.close()
        conn.close()

def reset_database(db_name, config_path, sql_file_path):
    if database_exists(db_name, config_path):
        delete_database(db_name, config_path)
        create_database(db_name, config_path, sql_file_path)
    else:
        print(f"Database '{db_name}' does not exist. Skipping reset.")

# --------------- LOAD CSV ----------------
def execute_sql_script(db_name, sql_file_path, config_path):
    config = load_config(config_path)
    conn = connect(database=db_name, user=config["user"], password=config["password"],
                   host=config["host"], port=config["port"])
    conn.autocommit = True
    cur = conn.cursor()
    try:
        with open(sql_file_path, "r", encoding="utf-8") as f:
            sql_script = f.read()
        cur.execute(sql_script)
        print(f"SQL script '{sql_file_path}' executed successfully on database '{db_name}'.")
        log_error(f"Database tables successfully build in Database '{db_name}'", "info")
    except Exception as e:
        log_error("Error executing SQL script", exception=e)
    finally:
        cur.close()
        conn.close()

from psycopg2 import connect, sql


def load_csv_to_table(db_name, table_name, csv_path, config_path):
    config = load_config(config_path)
    conn = connect(
        database=db_name,
        user=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"]
    )
    conn.autocommit = True
    cur = conn.cursor()

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            if table_name == "merged_distribution_metadata":
                # Spalten ohne 'distribution_identifier'
                columns = [
                    "dataset_identifier",
                    "distribution_format",
                    "distribution_access_url",
                    "origin",
                    "xml_filename",
                    "distribution_title_de",
                    "distribution_description_de",
                    "distribution_title_unknown",
                    "distribution_description_unknown",
                    "distribution_title_en",
                    "distribution_description_en",
                    "distribution_title_fr",
                    "distribution_description_fr",
                    "distribution_title_it",
                    "distribution_description_it",
                    "distribution_title_rm",
                    "distribution_description_rm",
                    "distribution_media_type",
                    "distribution_language",
                    "distribution_download_url",
                    "distribution_coverage",
                    "distribution_temporal_resolution",
                    "distribution_documentation",
                    "distribution_id",
                    "distribution_issued_date",
                    "distribution_modified_date",
                    "distribution_license",
                    "distribution_rights",
                    "distribution_byte_size",
                    "distribution_language_status_de",
                    "distribution_language_status_en",
                    "distribution_language_status_fr",
                    "distribution_language_status_it",
                    "distribution_language_status_unknown",
                    "distribution_language_quality",
                    "distribution_description_length_de",
                    "distribution_description_length_en",
                    "distribution_description_length_fr",
                    "distribution_description_length_it",
                    "distribution_description_length_rm",
                    "distribution_format_name",
                    "distribution_format_type",
                    "distribution_format_cluster",
                    "distribution_format_geodata",
                    "distribution_access_url_status_code",
                    "distribution_download_url_status_code"
                ]
                col_sql = sql.SQL(', ').join(map(sql.Identifier, columns))
                copy_stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER").format(
                    sql.Identifier(table_name),
                    col_sql
                )
            else:
                copy_stmt = sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(
                    sql.Identifier(table_name)
                )

            cur.copy_expert(copy_stmt, f)
            log_error(f"Successfully loaded CSV '{csv_path}' into the table '{table_name}'", "info")

    except Exception as e:
        log_error(f"Error loading CSV '{csv_path}' into table '{table_name}'", exception=e)
    finally:
        cur.close()
        conn.close()


def load_metadata(folder_path, config_path,db_name = "4M"):

    load_csv_to_table(db_name, "merged_dataset_metadata", os.path.join(folder_path, "merged_dataset_metadata.csv"), config_path)
    load_csv_to_table(db_name, "merged_contact_point_metadata", os.path.join(folder_path, "merged_contact_point_metadata.csv"), config_path)
    load_csv_to_table(db_name, "merged_distribution_metadata", os.path.join(folder_path, "merged_distribution_metadata.csv"), config_path)

# --------------- DELETE METADATA ----------------
def delete_metadata_by_filename(db_name, xml_filename, config_path):
    config = load_config(config_path)
    try:
        conn = psycopg2.connect(
            database=db_name,
            user=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"]
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT dataset_identifier FROM merged_dataset_metadata WHERE xml_filename = %s;", (xml_filename,))
        dataset_ids = cur.fetchall()
        if not dataset_ids:
            print(f"")
            log_error(f"No datasets found int the remove order to delete in the database", "info")
        else:
            log_error(f"Start deleting datasets from the remove order", "info")
            for dataset_id, in dataset_ids:
                cur.execute("DELETE FROM merged_dataset_metadata WHERE dataset_identifier = %s;", (dataset_id,))
            print(f"Deleted {len(dataset_ids)} dataset(s) for xml_filename = '{xml_filename}'")
        cur.close()
        conn.close()
        log_error(f"Successfully deleted all datasets from the remove order", "info")
    except Exception as e:
        log_error("Error deleting metadata entries", exception=e)


def delete_from_csv_list(csv_paths, db_name, config_path):
    if isinstance(csv_paths, str):
        csv_paths = [csv_paths]  # Ensure it's a list

    for csv_path in csv_paths:
        if not os.path.exists(csv_path):
            log_error(f"File not found, skipping: {csv_path}", exception=e)
            continue

        try:
            log_error(f"Start removing all requested data from: {csv_path}", "info")
            with open(csv_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    dataset_name = row.get('Dataset_Name')
                    if dataset_name:
                        xml_filename = f"{dataset_name}.xml"
                        delete_metadata_by_filename(db_name, xml_filename, config_path)
            log_error(f"Removing completed from: {csv_path}", "info")
        except Exception as e:
            log_error(f"Error processing CSV file for deletions: {csv_path}", exception=e)
    log_error(f"Removing endet:", "info")




# --------------- CLI ENTRY POINT ----------------
if __name__ == "__main__":
    log_start_message()

    parser = argparse.ArgumentParser(description="Manage metadata in PostgreSQL.")
    parser.add_argument("--folder", help="Folder containing CSV files to load")
    parser.add_argument("--config", required=True, help="Path to db_config.json")
    parser.add_argument("--sql", help="Path to SQL script to create tables")
    parser.add_argument("--csv", help="Path to CSV file containing filenames to delete")
    parser.add_argument("--db", help="PostgreSQL database name")
    args = parser.parse_args()

    if args.folder and args.sql:
        load_metadata(args.folder, args.config, args.sql)
    elif args.csv and args.db:
        delete_from_csv_list(args.csv, args.db, args.config)
    else:
        print("Please provide either --folder with --sql, or --csv with --db.")