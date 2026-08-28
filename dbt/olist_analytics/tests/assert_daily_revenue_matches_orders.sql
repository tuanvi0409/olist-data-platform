with expected_daily_revenue as (

    select
        order_purchase_date,

        sum(
            case
                when order_status = 'delivered'
                    then total_payment_value
                else 0
            end
        ) as expected_revenue

    from {{ ref('fct_orders') }}
    where order_purchase_date is not null
    group by order_purchase_date

),

actual_daily_revenue as (

    select
        order_purchase_date,
        delivered_revenue
    from {{ ref('mart_daily_sales') }}

),

invalid_dates as (

    select
        coalesce(
            expected.order_purchase_date,
            actual.order_purchase_date
        ) as order_purchase_date,

        expected.expected_revenue,
        actual.delivered_revenue,

        abs(
            coalesce(expected.expected_revenue, 0)
            - coalesce(actual.delivered_revenue, 0)
        ) as difference

    from expected_daily_revenue as expected
    full outer join actual_daily_revenue as actual
        on expected.order_purchase_date
        = actual.order_purchase_date

    where
        expected.order_purchase_date is null
        or actual.order_purchase_date is null
        or abs(
            coalesce(expected.expected_revenue, 0)
            - coalesce(actual.delivered_revenue, 0)
        ) > 0.01

)

select *
from invalid_dates