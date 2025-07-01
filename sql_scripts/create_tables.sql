CREATE TABLE IF NOT EXISTS temp_table (
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

CREATE TABLE IF NOT EXISTS nppes_cleaned (
	npi VARCHAR PRIMARY KEY,
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

CREATE UNIQUE INDEX npi_idx ON nppes_cleaned (npi);

-- CREATE VIEW merge_from AS
-- 	WITH filtered_tax_codes AS (
-- 		SELECT
-- 			npi,
-- 			entity_type_code,
-- 			provider_organization_name,
-- 			provider_last_name,
-- 			provider_first_name,
-- 			provider_middle_name,
-- 			provider_name_prefix,
-- 			provider_name_suffix,
-- 			provider_credentials,
-- 			provider_first_line_location_address,
-- 			provider_second_line_location_address,
-- 			provider_location_address_city_name,
-- 			provider_location_address_state_name,
-- 			provider_location_address_postal_code,
-- 			CASE WHEN taxonomy_switch_1 = 'Y' THEN taxonomy_code_1
-- 				WHEN taxonomy_switch_2 = 'Y' THEN taxonomy_code_2
-- 				WHEN taxonomy_switch_3 = 'Y' THEN taxonomy_code_3
-- 				WHEN taxonomy_switch_4 = 'Y' THEN taxonomy_code_4
-- 				WHEN taxonomy_switch_5 = 'Y' THEN taxonomy_code_5
-- 				WHEN taxonomy_switch_6 = 'Y' THEN taxonomy_code_6
-- 				WHEN taxonomy_switch_7 = 'Y' THEN taxonomy_code_7
-- 				WHEN taxonomy_switch_8 = 'Y' THEN taxonomy_code_8
-- 				WHEN taxonomy_switch_9 = 'Y' THEN taxonomy_code_9
-- 				WHEN taxonomy_switch_10 = 'Y' THEN taxonomy_code_10
-- 				WHEN taxonomy_switch_11 = 'Y' THEN taxonomy_code_11
-- 				WHEN taxonomy_switch_12 = 'Y' THEN taxonomy_code_12
-- 				WHEN taxonomy_switch_13 = 'Y' THEN taxonomy_code_13
-- 				WHEN taxonomy_switch_14 = 'Y' THEN taxonomy_code_14
-- 				WHEN taxonomy_switch_15 = 'Y' THEN taxonomy_code_15
-- 				ELSE null END AS taxonomy_code
-- 		FROM temp_table
-- 	)
-- 	SELECT 
-- 		ftc.npi,
-- 		ftc.entity_type_code,
-- 		ftc.provider_organization_name,
-- 		ftc.provider_last_name,
-- 		ftc.provider_first_name,
-- 		ftc.provider_middle_name,
-- 		ftc.provider_name_prefix,
-- 		ftc.provider_name_suffix,
-- 		ftc.provider_credentials,
-- 		ftc.provider_first_line_location_address,
-- 		ftc.provider_second_line_location_address,
-- 		ftc.provider_location_address_city_name,
-- 		ftc.provider_location_address_state_name,
-- 		ftc.provider_location_address_postal_code,
-- 		ftc.taxonomy_code,
-- 		codes.grouping,
-- 		codes.classification,
-- 		codes.specialization
-- 	FROM filtered_tax_codes ftc
-- 	LEFT JOIN nucc_taxonomy_250 codes
-- 	ON ftc.taxonomy_code = codes.code;
