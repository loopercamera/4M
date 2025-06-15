-- Main dataset table


    CREATE TABLE merged_dataset_metadata (
    dataset_identifier TEXT PRIMARY KEY,
    origin TEXT,
    xml_filename TEXT,
    dataset_language TEXT,

    dataset_title_DE TEXT,
    dataset_keyword_DE TEXT,
    dataset_description_DE TEXT,

    dataset_title_UNKNOWN TEXT,
    dataset_keyword_UNKNOWN TEXT,
    dataset_description_UNKNOWN TEXT,

    dataset_title_EN TEXT,
    dataset_keyword_EN TEXT,
    dataset_description_EN TEXT,

    dataset_keyword_FR TEXT,
    dataset_title_FR TEXT,
    dataset_description_FR TEXT,

    dataset_title_IT TEXT,
    dataset_keyword_IT TEXT,
    dataset_description_IT TEXT,

    dataset_title_RM TEXT,
    dataset_keyword_RM TEXT,
    dataset_description_RM TEXT,

    dataset_publisher_name TEXT,
    dataset_publisher_URL TEXT,

    dataset_spatial TEXT,
    dataset_theme TEXT,
    dataset_issued TEXT,

    dataset_is_mobility BOOLEAN,
    dataset_location_id TEXT,
    dataset_location TEXT,
    dataset_location_district TEXT,
    dataset_location_canton TEXT,
    dataset_location_country TEXT,

    dataset_language_status_de TEXT,
    dataset_language_status_en TEXT,
    dataset_language_status_fr TEXT,
    dataset_language_status_it TEXT,
    dataset_language_status_unknown TEXT,
    dataset_language_quality TEXT,

    dataset_description_length_de integer,
    dataset_description_length_en integer,
    dataset_description_length_fr integer,
    dataset_description_length_it integer,
    dataset_description_length_rm integer,
    dataset_distribution_format_count integer,
    dataset_keyword_count_de integer,
    dataset_keyword_count_en integer,
    dataset_keyword_count_fr integer,
    dataset_keyword_count_it integer,
    dataset_keyword_count_rm integer,
    dataset_cluster_id integer
);


-- 1-to-1 contact point table (dataset_identifier is also PRIMARY KEY here)
CREATE TABLE merged_contact_point_metadata (
    dataset_identifier TEXT,
    contact_type TEXT,
    contact_email TEXT,
    contact_name TEXT,
    origin TEXT,
    xml_filename TEXT,
    FOREIGN KEY (dataset_identifier) REFERENCES merged_dataset_metadata(dataset_identifier) ON DELETE CASCADE
);

    -- 1-to-many distribution table (multiple rows per dataset_identifier)
    CREATE TABLE merged_distribution_metadata (
    dataset_identifier TEXT,
    distribution_format TEXT,
    distribution_access_url TEXT,
    origin TEXT,
    xml_filename TEXT,
    distribution_title_DE TEXT,
    distribution_description_DE TEXT,
    distribution_title_UNKNOWN TEXT,
    distribution_description_UNKNOWN TEXT,
    distribution_title_EN TEXT,
    distribution_description_EN TEXT,
    distribution_title_FR TEXT,
    distribution_description_FR TEXT,
    distribution_title_IT TEXT,
    distribution_description_IT TEXT,
    distribution_title_RM TEXT,
    distribution_description_RM TEXT,
    distribution_media_type TEXT,
    distribution_language TEXT,
    distribution_download_url TEXT,
    distribution_coverage TEXT,
    distribution_temporal_resolution TEXT,
    distribution_documentation TEXT,
    distribution_id TEXT,
    distribution_issued_date TIMESTAMP,
    distribution_modified_date TIMESTAMP,
    distribution_license TEXT,
    distribution_rights TEXT,
    distribution_byte_size TEXT,

    distribution_language_status_de TEXT,
    distribution_language_status_en TEXT,
    distribution_language_status_fr TEXT,
    distribution_language_status_it TEXT,
    distribution_language_status_unknown TEXT,
    distribution_language_quality TEXT,

    distribution_description_length_de TEXT,
    distribution_description_length_en TEXT,
    distribution_description_length_fr TEXT,
    distribution_description_length_it TEXT,
    distribution_description_length_rm TEXT,

    distribution_format_name text,
    distribution_format_type text,
    distribution_format_cluster text,
    distribution_format_geodata boolean,
    distribution_access_url_status_code integer,
    distribution_download_url_status_code integer,
    distribution_identifier uuid DEFAULT gen_random_uuid() NOT NULL,

    FOREIGN KEY (dataset_identifier) REFERENCES merged_dataset_metadata(dataset_identifier) ON DELETE CASCADE
    );




