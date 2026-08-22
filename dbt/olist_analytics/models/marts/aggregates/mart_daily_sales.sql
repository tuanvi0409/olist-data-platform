with orders as (

    select *
    from {{ ref('fct_orders') }}

),

customers as (

    select
        customer_id,
        customer_unique_id
    from {{ ref('dim_customers') }}

),

orders_with_customers as (

    select
        orders.*,
        customers.customer_unique_id
    from orders
    left join customers
        on orders.customer_id = customers.customer_id

),

daily_sales as (

    select
        order_purchase_date,

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
        ) as late_delivery_count,

        avg(
            case
                when order_status = 'delivered'
                    then delivery_time_days
            end
        ) as average_delivery_time_days

    from orders_with_customers
    where order_purchase_date is not null
    group by order_purchase_date

)

select *
from daily_sales