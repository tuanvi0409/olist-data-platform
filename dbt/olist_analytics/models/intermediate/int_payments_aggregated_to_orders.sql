{{ config(materialized='view') }}

with payments as (

    select *
    from {{ ref('stg_order_payments') }}

),

aggregated as (

    select
        order_id,
        count(*) as payment_record_count,
        count(distinct payment_type) as payment_method_count,
        sum(payment_installments) as total_payment_installments,
        sum(payment_value) as total_payment_value
    from payments
    group by order_id

)

select *
from aggregated