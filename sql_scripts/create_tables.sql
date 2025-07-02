--Populated via python
CREATE TABLE IF NOT EXISTS staging_raw (
	npi VARCHAR,
  entity_type_code VARCHAR,
	provider_organization_name VARCHAR,
	provider_last_name VARCHAR,
	provider_first_name VARCHAR,
	provider_middle_name VARCHAR,
	provider_name_prefix VARCHAR,
	provider_name_suffix VARCHAR,
	provider_credentials VARCHAR,
	provider_first_line_location_address VARCHAR,
	provider_second_line_location_address VARCHAR,
	provider_location_address_city_name VARCHAR,
	provider_location_address_state_name VARCHAR,
	provider_location_address_postal_code VARCHAR,
	taxonomy_switch_1 VARCHAR,
	taxonomy_switch_2 VARCHAR,
	taxonomy_switch_3 VARCHAR,
	taxonomy_switch_4 VARCHAR,
	taxonomy_switch_5 VARCHAR,
	taxonomy_switch_6 VARCHAR,
	taxonomy_switch_7 VARCHAR,
	taxonomy_switch_8 VARCHAR,
	taxonomy_switch_9 VARCHAR,
	taxonomy_switch_10 VARCHAR,
	taxonomy_switch_11 VARCHAR,
	taxonomy_switch_12 VARCHAR,
	taxonomy_switch_13 VARCHAR,
	taxonomy_switch_14 VARCHAR,
	taxonomy_switch_15 VARCHAR,
	taxonomy_code_1 VARCHAR,
	taxonomy_code_2 VARCHAR,
	taxonomy_code_3 VARCHAR,
	taxonomy_code_4 VARCHAR,
	taxonomy_code_5 VARCHAR,
	taxonomy_code_6 VARCHAR,
	taxonomy_code_7 VARCHAR,
	taxonomy_code_8 VARCHAR,
	taxonomy_code_9 VARCHAR,
	taxonomy_code_10 VARCHAR,
	taxonomy_code_11 VARCHAR,
	taxonomy_code_12 VARCHAR,
	taxonomy_code_13 VARCHAR,
	taxonomy_code_14 VARCHAR,
	taxonomy_code_15 VARCHAR
);

--Import directly from CSV
CREATE TABLE IF NOT EXISTS nucc_taxonomy_250 (
	code VARCHAR PRIMARY KEY,
	grouping VARCHAR,
	classification VARCHAR,
	specialization VARCHAR,
	definition VARCHAR,
	notes VARCHAR,
	display_name VARCHAR,
	section VARCHAR
);

--clean_data() - Stored proc (in batches) with case statement to select taxonomy code
CREATE TABLE nppes_cleaned(
	npi INT PRIMARY KEY,
	entity_type_code VARCHAR,
	provider_organization_name VARCHAR,
	provider_last_name VARCHAR,
	provider_first_name VARCHAR,
	provider_middle_name VARCHAR,
	provider_name_prefix VARCHAR,
	provider_name_suffix VARCHAR,
	provider_credentials VARCHAR,
	provider_first_line_location_address VARCHAR,
	provider_second_line_location_address VARCHAR,
	provider_location_address_city_name VARCHAR,
	provider_location_address_state_name VARCHAR,
	provider_location_address_postal_code VARCHAR,
	taxonomy_code VARCHAR NULL REFERENCES nucc_taxonomy_250 (code)
);

CREATE INDEX code_idx ON nppes_cleaned (taxonomy_code);

--compile_data() - Populated via stored proc to join raw_with_single_code to nucc_taxonomy_250
CREATE TABLE nppes_compiled(
	npi INT PRIMARY KEY,
	entity_type_code VARCHAR,
	provider_organization_name VARCHAR,
	provider_last_name VARCHAR,
	provider_first_name VARCHAR,
	provider_middle_name VARCHAR,
	provider_name_prefix VARCHAR,
	provider_name_suffix VARCHAR,
	provider_credentials VARCHAR,
	provider_first_line_location_address VARCHAR,
	provider_second_line_location_address VARCHAR,
	provider_location_address_city_name VARCHAR,
	provider_location_address_state_name VARCHAR,
	provider_location_address_postal_code VARCHAR,
	taxonomy_code VARCHAR,
	grouping VARCHAR,
	classification VARCHAR,
	specialization VARCHAR
);

--Stored proc to merge cleaned data into final database
CREATE TABLE IF NOT EXISTS nppes_final (
	npi INT PRIMARY KEY,
	entity_type_code VARCHAR,
	provider_organization_name VARCHAR,
	provider_last_name VARCHAR,
	provider_first_name VARCHAR,
	provider_middle_name VARCHAR,
	provider_name_prefix VARCHAR,
	provider_name_suffix VARCHAR,
	provider_credentials VARCHAR,
	provider_first_line_location_address VARCHAR,
	provider_second_line_location_address VARCHAR,
	provider_location_address_city_name VARCHAR,
	provider_location_address_state_name VARCHAR,
	provider_location_address_postal_code VARCHAR,
	taxonomy_code VARCHAR,
	grouping VARCHAR,
	classification VARCHAR,
	specialization VARCHAR,
	created TIMESTAMP,
	last_modified TIMESTAMP
);
