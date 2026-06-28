/*
    stg_orders — Silver staging model
    ==================================
    Cleans and standardizes the raw Bronze orders table.
    One row per order_id.
*/

with source as (
    select * from {{ source('bronze', 'orders') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by order_id
            order by _ingested_at desc
        ) as _row_num
    from source
),

cleaned as (
    select
        cast(order_id as string)                as order_id,
        cast(customer_id as string)             as customer_id,
        lower(trim(status))                     as status,
        trim(shipping_city)                     as shipping_city,
        trim(shipping_country)                  as shipping_country,
        cast(discount_pct as decimal(5,2))      as discount_pct,
        cast(created_at as timestamp)           as created_at,
        cast(updated_at as timestamp)           as updated_at

    from deduplicated
    where _row_num = 1
)

select * from cleaned
