CREATE OR REPLACE PROCEDURE clean_data(IN batch_limit INT, IN batch_offset INT)
LANGUAGE plpgsql
AS $$
BEGIN
	TRUNCATE TABLE nppes_cleaned;

	INSERT INTO nppes_cleaned
	SELECT
		npi::INT,
		entity_type_code,
		provider_organization_name,
		provider_last_name,
		provider_first_name,
		provider_middle_name,
		provider_name_prefix,
		provider_name_suffix,
		provider_credentials,
		provider_first_line_location_address,
		provider_second_line_location_address,
		provider_location_address_city_name,
		provider_location_address_state_name,
		provider_location_address_postal_code,
		CASE WHEN taxonomy_switch_1 = 'Y' THEN taxonomy_code_1
			WHEN taxonomy_switch_2 = 'Y' THEN taxonomy_code_2
			WHEN taxonomy_switch_3 = 'Y' THEN taxonomy_code_3
			WHEN taxonomy_switch_4 = 'Y' THEN taxonomy_code_4
			WHEN taxonomy_switch_5 = 'Y' THEN taxonomy_code_5
			WHEN taxonomy_switch_6 = 'Y' THEN taxonomy_code_6
			WHEN taxonomy_switch_7 = 'Y' THEN taxonomy_code_7
			WHEN taxonomy_switch_8 = 'Y' THEN taxonomy_code_8
			WHEN taxonomy_switch_9 = 'Y' THEN taxonomy_code_9
			WHEN taxonomy_switch_10 = 'Y' THEN taxonomy_code_10
			WHEN taxonomy_switch_11 = 'Y' THEN taxonomy_code_11
			WHEN taxonomy_switch_12 = 'Y' THEN taxonomy_code_12
			WHEN taxonomy_switch_13 = 'Y' THEN taxonomy_code_13
			WHEN taxonomy_switch_14 = 'Y' THEN taxonomy_code_14
			WHEN taxonomy_switch_15 = 'Y' THEN taxonomy_code_15
			ELSE NULL END AS taxonomy_code
	FROM staging_raw
	ORDER BY npi
	LIMIT batch_limit
	OFFSET batch_offset;
END;
$$;

CREATE OR REPLACE PROCEDURE compile_data()
LANGUAGE plpgsql
AS $$
BEGIN
	TRUNCATE TABLE nppes_compiled;

	INSERT INTO nppes_compiled
	SELECT 
		clean.npi,
		clean.entity_type_code,
		clean.provider_organization_name,
		clean.provider_last_name,
		clean.provider_first_name,
		clean.provider_middle_name,
		clean.provider_name_prefix,
		clean.provider_name_suffix,
		clean.provider_credentials,
		clean.provider_first_line_location_address,
		clean.provider_second_line_location_address,
		clean.provider_location_address_city_name,
		clean.provider_location_address_state_name,
		clean.provider_location_address_postal_code,
		clean.taxonomy_code,
		codes.grouping,
		codes.classification,
		codes.specialization
	FROM nppes_cleaned clean
	LEFT JOIN nucc_taxonomy_250 codes
	ON clean.taxonomy_code = codes.code;
END;
$$;

CREATE OR REPLACE PROCEDURE store_data()
LANGUAGE plpgsql
AS $$
BEGIN
	MERGE INTO nppes_final target
	USING nppes_compiled source
	ON target.npi = source.npi
	WHEN MATCHED THEN
		UPDATE set entity_type_code = source.entity_type_code,
			provider_organization_name = source.provider_organization_name,
			provider_last_name = source.provider_last_name,
			provider_first_name = source.provider_first_name,
			provider_middle_name = source.provider_middle_name,
			provider_name_prefix = source.provider_name_prefix,
			provider_name_suffix = source.provider_name_suffix,
			provider_credentials = source.provider_credentials,
			provider_first_line_location_address = source.provider_first_line_location_address,
			provider_second_line_location_address = source.provider_second_line_location_address,
			provider_location_address_city_name = source.provider_location_address_city_name,
			provider_location_address_state_name = source.provider_location_address_state_name,
			provider_location_address_postal_code = source.provider_location_address_postal_code,
			taxonomy_code = source.taxonomy_code,
			grouping = source.grouping,
			classification = source.classification,
			specialization = source.specialization,
			last_modified = NOW()
	WHEN NOT MATCHED THEN
		INSERT (
			npi,
			entity_type_code,
			provider_organization_name,
			provider_last_name,
			provider_first_name,
			provider_middle_name,
			provider_name_prefix,
			provider_name_suffix,
			provider_credentials,
			provider_first_line_location_address,
			provider_second_line_location_address,
			provider_location_address_city_name,
			provider_location_address_state_name,
			provider_location_address_postal_code,
			taxonomy_code,
			grouping,
			classification,
			specialization,
			created,
			last_modified
		)
		VALUES(
			source.npi,
			source.entity_type_code,
			source.provider_organization_name,
			source.provider_last_name,
			source.provider_first_name,
			source.provider_middle_name,
			source.provider_name_prefix,
			source.provider_name_suffix,
			source.provider_credentials,
			source.provider_first_line_location_address,
			source.provider_second_line_location_address,
			source.provider_location_address_city_name,
			source.provider_location_address_state_name,
			source.provider_location_address_postal_code,
			source.taxonomy_code,
			source.grouping,
			source.classification,
			source.specialization,
			NOW(),
			NOW()
		);
END;
$$;
