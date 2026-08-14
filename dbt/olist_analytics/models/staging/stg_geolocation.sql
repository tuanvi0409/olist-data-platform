{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('olist_raw', 'geolocation') }}

),

renamed as (

    select
        cast(geolocation_zip_code_prefix as integer) as zip_code_prefix,
        cast(geolocation_lat as numeric(10, 7)) as latitude,
        cast(geolocation_lng as numeric(10, 7)) as longitude,
        trim(lower(geolocation_city)) as city,
        upper(trim(geolocation_state)) as state
    from source

)

select *
from renamed
