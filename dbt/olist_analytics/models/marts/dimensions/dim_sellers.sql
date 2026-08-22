with sellers as (

    select *
    from {{ ref('stg_sellers') }}

),

seller_dimension as (

    select
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    from sellers

)

select *
from seller_dimension