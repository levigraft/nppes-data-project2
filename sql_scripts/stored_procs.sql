CREATE OR REPLACE PROCEDURE clean_and_store()
LANGUAGE plpgsql
AS $$
BEGIN
	CREATE TEMP TABLE merge_from ON COMMIT DROP AS (
		WITH filtered_tax_codes AS (
			SELECT
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
					ELSE null END AS taxonomy_code
			FROM temp_table
		)
		SELECT 
			ftc.npi,
			ftc.entity_type_code,
			ftc.provider_organization_name,
			ftc.provider_last_name,
			ftc.provider_first_name,
			ftc.provider_middle_name,
			ftc.provider_name_prefix,
			ftc.provider_name_suffix,
			ftc.provider_credentials,
			ftc.provider_first_line_location_address,
			ftc.provider_second_line_location_address,
			ftc.provider_location_address_city_name,
			ftc.provider_location_address_state_name,
			ftc.provider_location_address_postal_code,
			ftc.taxonomy_code,
			codes.grouping,
			codes.classification,
			codes.specialization
		FROM filtered_tax_codes ftc
		LEFT JOIN nucc_taxonomy_250 codes
		ON ftc.taxonomy_code = codes.code
	);

	CREATE UNIQUE INDEX npi_temp_idx ON merge_from (npi);

	MERGE INTO nppes_cleaned target
	USING merge_from source
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
