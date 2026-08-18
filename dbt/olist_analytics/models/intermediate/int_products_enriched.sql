{{ config(materialized='view') }}

with products as (

    select *
    from {{ ref('stg_products') }}

),

category_translation as (

    select *
    from {{ ref('stg_product_category_translation') }}

),

products_enriched as (

    select
        products.product_id,
        products.product_category_name,
        category_translation.product_category_name_english,
        products.product_name_length,
        products.product_description_length,
        products.product_photos_quantity,
        products.product_weight_g,
        products.product_length_cm,
        products.product_height_cm,
        products.product_width_cm
    from products
    left join category_translation
        on products.product_category_name
        = category_translation.product_category_name

)

select *
from products_enriched