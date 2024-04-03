-- passenger_fact.sql
{{ config(
    materialized='table',
    schema='consumption'
) }}
WITH distinct_data AS (
    SELECT DISTINCT * FROM {{ ref('convert_passenger') }}
),

joined_data AS (
    SELECT
        d.activity_period,
        a.activity_id,
        c.terminal_id,
        g.geo_id,
        oa.airline_id AS operating_airline_id,
        oe.airline_id AS published_airline_id,
        t.passenger_count,
        p.price_id
    FROM
        distinct_data t
        JOIN {{ ref('date_dim') }} d ON CONCAT(t.activity_period, SUBSTRING(t.activity_period_start_date, 9, 2)) = d.activity_period
        JOIN {{ ref('activity_dim') }} a ON t.activity_type_code = a.activity_type_code
        JOIN {{ ref('terminal_dim') }} c ON CONCAT(t.boarding_area, '*', t.terminal) = c.boarding_terminal
        JOIN {{ ref('geo_dim') }} g ON t.geo_region = g.geo_region
        JOIN {{ ref('airline_dim') }} oa ON CONCAT(t.operating_airline, '*', t.operating_airline_iata_code) = oa.airline_code
        JOIN {{ ref('airline_dim') }} oe ON CONCAT(t.published_airline, '*', t.published_airline_iata_code) = oe.airline_code
        JOIN {{ ref('price_dim') }} p ON t.price_category_code = p.price_category_code
)
select * from joined_data
