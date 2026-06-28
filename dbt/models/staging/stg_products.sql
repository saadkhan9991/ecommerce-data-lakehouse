/*
    stg_products — Silver staging model
    ====================================
    Cleans and standardizes the raw Bronze products table.
    Renames `name` to `product_name` to avoid ambiguity in downstream joins.
*/

with source as (
    select * from {{ source('bronze', 'products') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by product_id
            order by _ingested_at desc
        ) as _row_num
    from source
),

cleaned as (
    select
        cast(product_id as string)              as product_id,
        trim(sku)                               as sku,
        trim(name)                              as product_name,
        lower(trim(category))                   as category,
        cast(price as decimal(10,2))            as price,
        cast(cost as decimal(10,2))             as cost,
        cast(supplier_id as string)             as supplier_id,
        cast(is_active as boolean)              as is_active,
        cast(created_at as timestamp)           as created_at,
        cast(updated_at as timestamp)           as updated_at

    from deduplicated
    where _row_num = 1
)

select * from cleaned
