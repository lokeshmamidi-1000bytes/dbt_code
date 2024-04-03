-- date_dim.sql
{{ config(
    materialized='table',
    schema='consumption'
) }}
WITH combined_data AS (
    SELECT activity_period, activity_period_start_date FROM {{ ref('convert_cargo') }}
    UNION
    SELECT activity_period, activity_period_start_date FROM {{ ref('convert_landing') }}
    UNION
    SELECT activity_period, activity_period_start_date FROM {{ ref('convert_passenger') }}
),

ranked_data AS (
    SELECT 
        CONCAT(activity_period, SUBSTRING(activity_period_start_date, 9, 2)) AS activity_period,
        activity_period_start_date,
        EXTRACT(DAY FROM activity_period_start_date) AS day_of_month,
        EXTRACT(MONTH FROM activity_period_start_date) AS month_of_year,
        EXTRACT(YEAR FROM activity_period_start_date) AS year
    FROM combined_data
)
SELECT * FROM ranked_data