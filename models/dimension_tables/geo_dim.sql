-- geography_dim.sql
{{ config(
    materialized='table',
    schema='consumption'
) }}
WITH combined_data AS (
    SELECT geo_region, geo_summary FROM {{ ref('convert_cargo') }}
    UNION 
    SELECT geo_region, geo_summary FROM {{ ref('convert_passenger') }}
    UNION 
    SELECT geo_region, geo_summary FROM {{ ref('convert_landing') }}
),

ranked_data AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY geo_region) AS geo_id, 
        geo_region, 
        geo_summary
    FROM combined_data
)

SELECT * FROM ranked_data
