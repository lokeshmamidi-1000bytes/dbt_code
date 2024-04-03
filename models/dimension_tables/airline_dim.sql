-- airline_dim.sql
{{ config(
    materialized='table',
    schema='consumption'
) }}
WITH combined_data AS (
    SELECT operating_airline AS airline, operating_airline_iata_code AS airline_iata_code
    FROM {{ ref('convert_cargo') }}

    UNION ALL

    SELECT operating_airline AS airline, operating_airline_iata_code AS airline_iata_code
    FROM {{ ref('convert_landing') }}

    UNION ALL

    SELECT operating_airline AS airline, operating_airline_iata_code AS airline_iata_code
    FROM {{ ref('convert_passenger') }}

    UNION ALL

    SELECT published_airline AS airline, published_airline_iata_code AS airline_iata_code
    FROM {{ ref('convert_cargo') }}

    UNION ALL

    SELECT published_airline AS airline, published_airline_iata_code AS airline_iata_code
    FROM {{ ref('convert_landing') }}

    UNION ALL

    SELECT published_airline AS airline, published_airline_iata_code AS airline_iata_code
    FROM {{ ref('convert_passenger') }}
),

ranked_data AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY airline, airline_iata_code) AS airline_id,
        airline,
        airline_iata_code,
        airline || '*' || airline_iata_code AS airline_code
    FROM combined_data
    GROUP BY  
        airline, airline_iata_code
)
SELECT * FROM ranked_data

