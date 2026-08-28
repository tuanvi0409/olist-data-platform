with order_item_totals as (

    select
        order_id,
        sum(total_item_value) as calculated_order_value
    from {{ ref('fct_order_items') }}
    group by order_id

),

invalid_orders as (

    select
        orders.order_id,
        orders.total_order_value,
        coalesce(
            order_item_totals.calculated_order_value,
            0
        ) as calculated_order_value,

        abs(
            orders.total_order_value
            - coalesce(
                order_item_totals.calculated_order_value,
                0
            )
        ) as difference

    from {{ ref('fct_orders') }} as orders
    left join order_item_totals
        on orders.order_id = order_item_totals.order_id

    where abs(
        orders.total_order_value
        - coalesce(
            order_item_totals.calculated_order_value,
            0
        )
    ) > 0.01

)

select *
from invalid_orders