with orders as (

    select *
    from {{ ref('int_orders_enriched') }}

),

order_facts as (

    select
        order_id,
        customer_id,
        order_status,

        order_purchased_at,
        order_approved_at,
        delivered_to_carrier_at,
        delivered_to_customer_at,
        estimated_delivery_at,

        cast(order_purchased_at as date) as order_purchase_date,

        item_count,
        unique_product_count,
        seller_count,

        total_item_value,
        total_freight_value,
        total_order_value,

        payment_record_count,
        payment_method_count,
        total_payment_installments,
        total_payment_value,

        delivery_time_days,
        delivery_delay_days,

        case
            when order_status = 'delivered' then true
            else false
        end as is_delivered,

        case
            when delivery_delay_days > 0 then true
            else false
        end as is_late_delivery

    from orders

)

select *
from order_facts