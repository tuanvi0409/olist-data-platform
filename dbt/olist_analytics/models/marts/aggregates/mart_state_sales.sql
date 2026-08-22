with orders as (

    select *
    from {{ ref('fct_orders') }}

),

customers as (

    select *
    from {{ ref('dim_customers') }}

),

orders_with_location as (

    select
        orders.order_id,
        orders.order_status,
        orders.total_payment_value,
        orders.total_freight_value,
        orders.is_late_delivery,
        customers.customer_unique_id,
        customers.customer_state
    from orders
    left join customers
        on orders.customer_id = customers.customer_id

),

state_sales as (

    select
        customer_state,

        count(*) as order_count,

        count(
            distinct customer_unique_id
        ) as unique_customer_count,

        sum(
            case
                when order_status = 'delivered' then 1
                else 0
            end
        ) as delivered_order_count,

        sum(
            case
                when order_status = 'canceled' then 1
                else 0
            end
        ) as canceled_order_count,

        sum(
            case
                when order_status = 'delivered'
                    then total_payment_value
                else 0
            end
        ) as delivered_revenue,

        sum(
            case
                when order_status = 'delivered'
                    then total_freight_value
                else 0
            end
        ) as delivered_freight_value,

        sum(
            case
                when order_status = 'delivered'
                     and is_late_delivery = true
                    then 1
                else 0
            end
        ) as late_delivery_count

    from orders_with_location
    where customer_state is not null
    group by customer_state

)

select *
from state_sales