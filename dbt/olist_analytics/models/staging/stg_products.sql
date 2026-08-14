{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('olist_raw', 'products') }}

),

renamed as (

    select
        product_id,
        trim(lower(product_category_name)) as product_category_name,
        cast(product_name_lenght as integer) as product_name_length,
        cast(product_description_lenght as integer) as product_description_length,
        cast(product_photos_qty as integer) as product_photos_quantity,
        cast(product_weight_g as numeric(12, 2)) as product_weight_g,
        cast(product_length_cm as numeric(12, 2)) as product_length_cm,
        cast(product_height_cm as numeric(12, 2)) as product_height_cm,
        cast(product_width_cm as numeric(12, 2)) as product_width_cm
    from source

)

select *
from renamed