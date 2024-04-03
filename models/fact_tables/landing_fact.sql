-- landing_fact.sql
{{ config(
    materialized='table',
    schema='consumption'
) }}
WITH distinct_data AS (
    SELECT DISTINCT * FROM {{ ref('convert_landing') }}
),

joined_data AS (
    SELECT
        d.activity_period,
        a.aircraft_id,
        g.geo_id,
        t.landing_count,
        oa.airline_id AS operating_airline_id,
        oe.airline_id AS published_airline_id,
        t.total_landed_weight
    FROM
        distinct_data t
        JOIN {{ ref('date_dim') }} d ON CONCAT(t.activity_period, SUBSTRING(t.activity_period_start_date, 9, 2)) = d.activity_period
        JOIN {{ ref('aircraft_dim') }} a ON CONCAT(t.aircraft_model, '*', t.aircraft_version, '*', t.landing_aircraft_type) = a.aircraft_model_version_type
        JOIN {{ ref('geo_dim') }} g ON t.geo_region = g.geo_region
        JOIN {{ ref('airline_dim') }} oa ON CONCAT(t.operating_airline, '*', t.operating_airline_iata_code) = oa.airline_code
        JOIN {{ ref('airline_dim') }} oe ON CONCAT(t.published_airline, '*', t.published_airline_iata_code) = oe.airline_code
)
select * from joined_data
