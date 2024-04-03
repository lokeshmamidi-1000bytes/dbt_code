{{ config(
    materialized='table',
    schema='consumption'
) }}
WITH grouped_data AS (
    SELECT  
        cargo_aircraft_type,  
        cargo_type_code,  
        CONCAT(cargo_aircraft_type, '*', cargo_type_code) AS cargo_aircraft_code, 
        ROW_NUMBER() OVER (ORDER BY CONCAT(cargo_aircraft_type, '*', cargo_type_code)) AS cargo_id 
    FROM  {{ ref('convert_cargo') }}
    GROUP BY  
        cargo_aircraft_type,  
        cargo_type_code
)
SELECT * FROM grouped_data