with parsed_json as (
    select
        parse_json(DATA) as json_data
    from raw.cargo
)



{{ config(
    materialized='table',
    schema='land',
    alias='cargo_csv'
) }}

select
    json_data:activity_period::string as activity_period,
    to_timestamp(json_data:activity_period_start_date::string) as activity_period_start_date,
    json_data:activity_type_code::string as activity_type_code,
    json_data:cargo_aircraft_type::string as cargo_aircraft_type,
    json_data:cargo_metric_tons::float as cargo_metric_tons,
    json_data:cargo_type_code::string as cargo_type_code,
    json_data:cargo_weight_lbs::int as cargo_weight_lbs,
    to_timestamp(json_data:data_as_of::string) as data_as_of,
    to_timestamp(json_data:data_loaded_at::string) as data_loaded_at,
    json_data:geo_region::string as geo_region,
    -- Update geo_summary based on the conditions
    case 
        when json_data:geo_region::string = 'US' and json_data:geo_summary::string = 'International' then 'Domestic'
        when json_data:geo_region::string = 'Europe' and json_data:geo_summary::string = 'Domestic' then 'International'
        else json_data:geo_summary::string
    end as geo_summary,
    json_data:operating_airline::string as operating_airline,
    json_data:published_airline::string as published_airline,
    -- Replace null values in operating_airline_iata_code with the first letter of each word in operating_airline
    case 
        when json_data:operating_airline_iata_code::string is null then 
            concat(
                substring(json_data:operating_airline::string, 1, 1),
                case when position(' ' in json_data:operating_airline::string) > 0 then 
                    substring(json_data:operating_airline::string, position(' ' in json_data:operating_airline::string) + 1, 1)
                else '' end
            )
        else json_data:operating_airline_iata_code::string
    end as operating_airline_iata_code,
    -- Replace null values in published_airline_iata_code with the first letter of each word in published_airline
    case 
        when json_data:published_airline_iata_code::string is null then 
            concat(
                substring(json_data:published_airline::string, 1, 1),
                case when position(' ' in json_data:published_airline::string) > 0 then 
                    substring(json_data:published_airline::string, position(' ' in json_data:published_airline::string) + 1, 1)
                else '' end
            )
        else json_data:published_airline_iata_code::string
    end as published_airline_iata_code
from parsed_json
