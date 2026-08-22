with order_items as (

    select *
    from {{ ref('fct_order_items') }}

),

products as (

    select *
    from {{ ref('dim_products') }}

),

items_with_categories as (

    select
        order_items.order_id,
        order_items.product_id,
        order_items.seller_id,
        order_items.order_status,
        order_items.item_price,
        order_items.freight_value,
        order_items.total_item_value,

        coalesce(
            products.product_category_name,
            'unknown'
        ) as product_category_name,

        coalesce(
            products.product_category_name_english,
            'unknown'
        ) as product_category_name_english

    from order_items
    left join products
        on order_items.product_id = products.product_id

),

category_sales as (

    select
        product_category_name,
        product_category_name_english,

        count(*) as item_count,
        count(distinct order_id) as order_count,
        count(distinct product_id) as unique_product_count,
        count(distinct seller_id) as seller_count,

        sum(item_price) as total_item_revenue,
        sum(freight_value) as total_freight_value,
        sum(total_item_value) as total_sales_value,

        avg(item_price) as average_item_price

    from items_with_categories
    where order_status = 'delivered'
    group by
        product_category_name,
        product_category_name_english

)

select *
from category_sales