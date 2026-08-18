{{ config(materialized='view') }}

with orders as (

    select *
    from {{ ref('stg_orders') }}

),

order_items as (

    select *
    from {{ ref('int_order_items_aggregated_to_orders') }}

),

payments as (

    select *
    from {{ ref('int_payments_aggregated_to_orders') }}

),

orders_enriched as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_status,
        orders.order_purchased_at,
        orders.order_approved_at,
        orders.delivered_to_carrier_at,
        orders.delivered_to_customer_at,
        orders.estimated_delivery_at,

        coalesce(order_items.item_count, 0) as item_count,
        coalesce(order_items.unique_product_count, 0) as unique_product_count,
        coalesce(order_items.seller_count, 0) as seller_count,
        coalesce(order_items.total_item_value, 0) as total_item_value,
        coalesce(order_items.total_freight_value, 0) as total_freight_value,
        coalesce(order_items.total_order_value, 0) as total_order_value,

        coalesce(payments.payment_record_count, 0) as payment_record_count,
        coalesce(payments.payment_method_count, 0) as payment_method_count,
        coalesce(payments.total_payment_installments, 0) as total_payment_installments,
        coalesce(payments.total_payment_value, 0) as total_payment_value,

        extract(
            epoch from (
                orders.delivered_to_customer_at
                - orders.order_purchased_at
            )
        ) / 86400.0 as delivery_time_days,

        extract(
            epoch from (
                orders.delivered_to_customer_at
                - orders.estimated_delivery_at
            )
        ) / 86400.0 as delivery_delay_days

    from orders
    left join order_items
        on orders.order_id = order_items.order_id
    left join payments
        on orders.order_id = payments.order_id

)

select *
from orders_enriched