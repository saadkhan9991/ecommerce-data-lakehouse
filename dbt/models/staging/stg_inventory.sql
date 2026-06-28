/*
    stg_inventory — Silver staging model
    =====================================
    Cleans and standardizes the raw Bronze inventory table.
    One row per inventory_id (unique product-warehouse combination).
*/

with source as (
    select * from {{ source('bronze', 'inventory') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by inventory_id
            order by _ingested_at desc
        ) as _row_num
    from source
),

cleaned as (
    select
        cast(inventory_id as string)            as inventory_id,
        cast(product_id as string)              as product_id,
        trim(warehouse)                         as warehouse,
        cast(quantity as int)                    as quantity,
        cast(reorder_level as int)              as reorder_level,
        cast(updated_at as timestamp)           as updated_at

    from deduplicated
    where _row_num = 1
)

select * from cleaned
