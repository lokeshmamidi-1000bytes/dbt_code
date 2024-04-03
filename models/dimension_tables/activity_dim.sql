-- activity_dim.sql
{{ config(
    materialized='table',
    schema='consumption'
) }}
WITH combined_data AS (
    SELECT activity_type_code FROM {{ ref('convert_cargo') }}
    UNION
    SELECT activity_type_code FROM {{ ref('convert_passenger') }}
),

ranked_data AS (
    SELECT DISTINCT activity_type_code,  
       ROW_NUMBER() OVER (ORDER BY activity_type_code) AS activity_id 
    FROM combined_data
)
SELECT * FROM ranked_data