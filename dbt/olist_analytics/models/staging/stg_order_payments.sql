{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('olist_raw', 'order_payments') }}

),

renamed as (

    select
        order_id,
        cast(payment_sequential as integer) as payment_sequence,
        trim(lower(payment_type)) as payment_type,
        cast(payment_installments as integer) as payment_installments,
        cast(payment_value as numeric(12, 2)) as payment_value
    from source

)

select *
from renamed