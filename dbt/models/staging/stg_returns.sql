/*
    stg_returns — Silver staging model
    ====================================
    Cleans and standardizes the raw Bronze returns table.
    One row per return_id.
*/

with source as (
    select * from {{ source('bronze', 'returns') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by return_id
            order by _ingested_at desc
        ) as _row_num
    from source
),

cleaned as (
    select
        cast(return_id as string)               as return_id,
        cast(order_item_id as string)           as order_item_id,
        lower(trim(reason))                     as reason,
        cast(refund_amount as decimal(10,2))    as refund_amount,
        cast(created_at as timestamp)           as created_at

    from deduplicated
    where _row_num = 1
)

select * from cleaned
