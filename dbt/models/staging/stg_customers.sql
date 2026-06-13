/*
    stg_customers — Silver staging model
    =====================================
    Cleans and standardizes the raw Bronze customers table.

    What this model does:
      1. Reads from the Bronze source (raw Postgres mirror on Iceberg)
      2. Deduplicates by customer_id (keeps the latest ingestion)
      3. Trims whitespace on string columns
      4. Lowercases email for consistency
      5. Casts types explicitly (no implicit conversions downstream)
      6. Drops Bronze lineage columns (_ingested_at, _source)

    After this model, downstream consumers can trust:
      - Every customer_id appears exactly once
      - Emails are lowercase and trimmed
      - No leading/trailing whitespace in names or cities
      - Types are explicit and predictable
*/

with source as (

    select * from {{ source('bronze', 'customers') }}

),

-- Deduplicate: if the same customer_id was ingested multiple times
-- (e.g., from CDC or re-runs), keep only the most recent row.
-- Uses _ingested_at as the tiebreaker — the Bronze lineage column
-- we added during ingestion.
deduplicated as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        -- Primary key
        cast(customer_id as string)             as customer_id,

        -- Names: trim whitespace
        trim(first_name)                        as first_name,
        trim(last_name)                         as last_name,

        -- Email: trim + lowercase for consistency
        lower(trim(email))                      as email,

        -- Phone: trim, keep as string (phones have leading zeros, dashes)
        trim(phone)                             as phone,

        -- Location: trim
        trim(city)                              as city,
        trim(country)                           as country,

        -- Segment: trim + lowercase for consistent filtering
        lower(trim(segment))                    as segment,

        -- Timestamps: cast explicitly
        cast(created_at as timestamp)           as created_at,
        cast(updated_at as timestamp)           as updated_at

    from deduplicated
    where _row_num = 1  -- keep only the latest version of each customer

)

select * from cleaned