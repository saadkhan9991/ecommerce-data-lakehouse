/*
    stg_suppliers — Silver staging model
    =====================================
    Cleans and standardizes the raw Bronze suppliers table.
    What this model does:
      1. Reads from the Bronze source (raw Postgres mirror on Iceberg)
      2. Deduplicates by supplier_id (keeps the latest ingestion)
      3. Trims whitespace on string columns
      4. Lowercases email for consistency
      5. Casts types explicitly
      6. Drops Bronze lineage columns (_ingested_at, _source)
*/

with source as (
    select * from {{ source('bronze', 'suppliers') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by supplier_id
            order by _ingested_at desc
        ) as _row_num
    from source
),

cleaned as (
    select
        -- Primary key
        cast(supplier_id as string)             as supplier_id,

        -- Business fields: trim whitespace
        trim(name)                              as supplier_name,
        lower(trim(contact_email))              as contact_email,
        trim(country)                           as country,

        -- Numeric: cast explicitly
        cast(lead_time_days as int)             as lead_time_days,

        -- Timestamps: cast explicitly
        cast(created_at as timestamp)           as created_at

    from deduplicated
    where _row_num = 1
)

select * from cleaned
