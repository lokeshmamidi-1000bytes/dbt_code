-- aircraft_dim.sql
{{ config(
    materialized='table',
    schema='consumption'
) }}
WITH ranked_data AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY aircraft_manufacturer, aircraft_model, aircraft_version, aircraft_body_type, landing_aircraft_type) AS aircraft_id, 
        aircraft_manufacturer, 
        aircraft_model, 
        aircraft_version,
        aircraft_body_type,
        landing_aircraft_type,
        CONCAT(aircraft_model, '*', aircraft_version,'*', landing_aircraft_type ) AS aircraft_model_version_type 
    FROM {{ ref('convert_landing') }}
    GROUP BY  
        aircraft_manufacturer, aircraft_model, aircraft_version, aircraft_body_type, landing_aircraft_type
)
SELECT * FROM ranked_data
