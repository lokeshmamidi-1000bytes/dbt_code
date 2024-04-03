WITH parsed_json AS (
    SELECT DISTINCT
        parse_json(DATA) AS json_data
    FROM raw.landing
)

{{ config(
    materialized='table',
    schema='land',
    alias='landing_csv'
) }}

SELECT
    activity_period,
    activity_period_start_date,
    aircraft_body_type,
    aircraft_manufacturer,
    aircraft_model,
    aircraft_version,
    data_as_of,
    data_loaded_at,
    geo_region,
    geo_summary,
    landing_aircraft_type,
    operating_airline,
    published_airline,
    operating_airline_iata_code,
    published_airline_iata_code,
    SUM(landing_count) AS landing_count,
    SUM(total_landed_weight) AS total_landed_weight
FROM (
    SELECT
        json_data:activity_period::STRING AS activity_period,
        to_timestamp(json_data:activity_period_start_date::STRING) AS activity_period_start_date,
        CASE 
            WHEN json_data:aircraft_model::STRING = '757' AND json_data:aircraft_body_type::STRING = 'Wide Body' THEN 'Narrow Body'
            WHEN json_data:aircraft_model::STRING = 'A333' AND json_data:aircraft_body_type::STRING = 'Narrow Body' THEN 'Wide Body'
            WHEN json_data:aircraft_model::STRING = 'B763' AND json_data:aircraft_body_type::STRING = 'Narrow Body' THEN 'Wide Body'
            WHEN json_data:aircraft_model::STRING = 'B789' AND json_data:aircraft_body_type::STRING = 'Narrow Body' THEN 'Wide Body'
            WHEN json_data:aircraft_model::STRING = 'DC-8' AND json_data:aircraft_body_type::STRING = 'Narrow Body' THEN 'Wide Body'
            WHEN json_data:aircraft_model::STRING = 'ERJ' AND json_data:aircraft_body_type::STRING = 'Turbo Prop' THEN 'Regional Jet'
            ELSE json_data:aircraft_body_type::STRING
        END AS aircraft_body_type,
        json_data:aircraft_manufacturer::STRING AS aircraft_manufacturer,
        json_data:aircraft_model::STRING AS aircraft_model,
        CASE 
            WHEN json_data:aircraft_version::STRING IS NULL OR json_data:aircraft_version::STRING = '-' THEN 'n/a'
            ELSE json_data:aircraft_version::STRING
        END AS aircraft_version,
        to_timestamp(json_data:data_as_of::STRING) AS data_as_of,
        to_timestamp(json_data:data_loaded_at::STRING) AS data_loaded_at,
        json_data:geo_region::STRING AS geo_region,
        CASE 
            WHEN json_data:geo_region::STRING = 'US' AND json_data:geo_summary::STRING = 'International' THEN 'Domestic'
            WHEN json_data:geo_region::STRING = 'Europe' AND json_data:geo_summary::STRING = 'Domestic' THEN 'International'
            ELSE json_data:geo_summary::STRING
        END AS geo_summary,
        json_data:landing_aircraft_type::STRING AS landing_aircraft_type,
        json_data:landing_count::INT AS landing_count,
        json_data:operating_airline::STRING AS operating_airline,
        json_data:published_airline::STRING AS published_airline,
        CASE 
            WHEN json_data:operating_airline_iata_code::STRING IS NULL THEN 
                CONCAT(
                    SUBSTRING(json_data:operating_airline::STRING, 1, 1),
                    CASE 
                        WHEN POSITION(' ' IN json_data:operating_airline::STRING) > 0 THEN 
                            SUBSTRING(json_data:operating_airline::STRING, POSITION(' ' IN json_data:operating_airline::STRING) + 1, 1)
                        ELSE '' 
                    END
                )
            ELSE json_data:operating_airline_iata_code::STRING
        END AS operating_airline_iata_code,
        CASE 
            WHEN json_data:published_airline_iata_code::STRING IS NULL THEN 
                CONCAT(
                    SUBSTRING(json_data:published_airline::STRING, 1, 1),
                    CASE 
                        WHEN POSITION(' ' IN json_data:published_airline::STRING) > 0 THEN 
                            SUBSTRING(json_data:published_airline::STRING, POSITION(' ' IN json_data:published_airline::STRING) + 1, 1)
                        ELSE '' 
                    END
                )
            ELSE json_data:published_airline_iata_code::STRING
        END AS published_airline_iata_code,
        json_data:total_landed_weight::INT AS total_landed_weight
    FROM parsed_json
) AS subquery
GROUP BY
    activity_period,
    activity_period_start_date,
    aircraft_body_type,
    aircraft_manufacturer,
    aircraft_model,
    aircraft_version,
    data_as_of,
    data_loaded_at,
    geo_region,
    geo_summary,
    landing_aircraft_type,
    operating_airline,
    published_airline,
    operating_airline_iata_code,
    published_airline_iata_code
