-- terminal_dim.sql
{{ config(
    materialized='table',
    schema='consumption'
) }}
WITH grouped_data AS (
    SELECT  
        boarding_area,  
        terminal, 
        CONCAT(boarding_area, '*', terminal) AS boarding_terminal, 
        ROW_NUMBER() OVER (ORDER BY boarding_area, terminal) AS terminal_id 
    FROM  {{ ref('convert_passenger') }}
    GROUP BY  
        boarding_area,  
        terminal
)
SELECT * FROM grouped_data
