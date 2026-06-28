/*
    stg_order_items — Silver staging model
    =======================================
    Cleans and standardizes the raw Bronze order_items table.
    One row per order_item_id (one line item in an order).
*/

with source as (
    select * from {{ source('bronze', 'order_items') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by order_item_id
            order by _ingested_at desc
        ) as _row_num
    from source
),

cleaned as (
    select
        cast(order_item_id as string)           as order_item_id,
        cast(order_id as string)                as order_id,
        cast(product_id as string)              as product_id,
        cast(quantity as int)                    as quantity,
        cast(unit_price as decimal(10,2))       as unit_price,
        cast(created_at as timestamp)           as created_at

    from deduplicated
    where _row_num = 1
)

select * from cleaned
