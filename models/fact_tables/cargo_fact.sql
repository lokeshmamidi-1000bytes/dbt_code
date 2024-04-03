-- cargo_fact.sql
{{ config(
    materialized='table',
    schema='consumption'
) }}
WITH distinct_data AS (
    SELECT DISTINCT * FROM {{ ref('convert_cargo') }}
),

joined_data AS (
    SELECT
        d.activity_period,
        a.activity_id,
        c.cargo_id,
        g.geo_id,
        oa.airline_id AS operating_airline_id,
        oe.airline_id AS published_airline_id,
        t.cargo_metric_tons,
        t.cargo_weight_lbs
    FROM
        distinct_data t
        JOIN {{ ref('date_dim') }} d ON CONCAT(t.activity_period, SUBSTRING(t.activity_period_start_date, 9, 2)) = d.activity_period
        JOIN {{ ref('activity_dim') }} a ON t.activity_type_code = a.activity_type_code
        JOIN {{ ref('cargo_dim') }} c ON CONCAT(t.cargo_aircraft_type, '*', t.cargo_type_code) = c.cargo_aircraft_code
        JOIN {{ ref('geo_dim') }} g ON t.geo_region = g.geo_region
        JOIN {{ ref('airline_dim') }} oa ON CONCAT(t.operating_airline, '*', t.operating_airline_iata_code) = oa.airline_code
        JOIN {{ ref('airline_dim') }} oe ON CONCAT(t.published_airline, '*', t.published_airline_iata_code) = oe.airline_code
)
select * from joined_data
