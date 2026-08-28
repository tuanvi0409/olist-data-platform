select
    order_id,
    item_count,
    total_item_value,
    total_freight_value,
    total_order_value,
    total_payment_value
from {{ ref('fct_orders') }}
where
    item_count < 0
    or total_item_value < 0
    or total_freight_value < 0
    or total_order_value < 0
    or total_payment_value < 0