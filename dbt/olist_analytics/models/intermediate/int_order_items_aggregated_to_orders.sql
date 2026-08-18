{{ config(materialized='view') }}

with order_items as (

    select *
    from {{ ref('stg_order_items') }}

),

aggregated as (

    select
        order_id,
        count(*) as item_count,
        count(distinct product_id) as unique_product_count,
        count(distinct seller_id) as seller_count,
        sum(item_price) as total_item_value,
        sum(freight_value) as total_freight_value,
        sum(item_price + freight_value) as total_order_value
    from order_items
    group by order_id

)

select *
from aggregated