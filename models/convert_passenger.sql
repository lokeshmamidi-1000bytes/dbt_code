WITH parsed_json AS (
    SELECT
        parse_json(DATA) AS json_data
    FROM raw.passenger
)

{{ config(
    materialized='table',
    schema='land',
    alias='passenger_csv'
) }}

SELECT
    json_data:activity_period::STRING AS activity_period,
    to_timestamp(json_data:activity_period_start_date::STRING) AS activity_period_start_date,
    json_data:activity_type_code::STRING AS activity_type_code,
    json_data:boarding_area::STRING AS boarding_area,
    to_timestamp(json_data:data_as_of::STRING) AS data_as_of,
    to_timestamp(json_data:data_loaded_at::STRING) AS data_loaded_at,
    json_data:geo_region::STRING AS geo_region,
    -- Update geo_summary based on the conditions
    CASE 
        WHEN json_data:geo_region::STRING = 'US' AND json_data:geo_summary::STRING = 'International' THEN 'Domestic'
        WHEN json_data:geo_region::STRING = 'Europe' AND json_data:geo_summary::STRING = 'Domestic' THEN 'International'
        ELSE json_data:geo_summary::STRING
    END AS geo_summary,
    json_data:operating_airline::STRING AS operating_airline,
    json_data:published_airline::STRING AS published_airline,
    -- Replace null values in operating_airline_iata_code with the first letter of each word in operating_airline
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
    -- Replace null values in published_airline_iata_code with the first letter of each word in published_airline
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
    json_data:passenger_count::INT AS passenger_count,
    json_data:price_category_code::STRING AS price_category_code,
    json_data:terminal::STRING AS terminal
FROM parsed_json
