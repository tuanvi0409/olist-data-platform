with order_items as (

    select *
    from {{ ref('stg_order_items') }}

),

orders as (

    select
        order_id,
        customer_id,
        order_status,
        order_purchased_at
    from {{ ref('stg_orders') }}

),

order_item_facts as (

    select
        order_items.order_id
            || '-'
            || cast(order_items.order_item_id as text)
            as order_item_key,

        order_items.order_id,
        order_items.order_item_id,
        orders.customer_id,
        order_items.product_id,
        order_items.seller_id,

        orders.order_status,
        orders.order_purchased_at,
        cast(orders.order_purchased_at as date) as order_purchase_date,
        order_items.shipping_limit_at,

        order_items.item_price as item_price,
        order_items.freight_value,

        order_items.item_price
            + order_items.freight_value
            as total_item_value

    from order_items
    inner join orders
        on order_items.order_id = orders.order_id

)

select *
from order_item_facts