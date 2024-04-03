{{ config(
    materialized='table',
    schema='consumption'
) }}
WITH unique_categories AS (
    SELECT DISTINCT price_category_code 
    FROM {{ ref('convert_passenger') }}
),

ranked_data AS (
    SELECT  
        price_category_code, 
        ROW_NUMBER() OVER (ORDER BY price_category_code) AS price_id 
    FROM unique_categories
)
SELECT * FROM ranked_data