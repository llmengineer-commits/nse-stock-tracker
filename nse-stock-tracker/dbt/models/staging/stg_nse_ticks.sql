-- models/staging/stg_nse_ticks.sql
--
-- Cleans and casts raw_ticks into a well-typed staging view.
-- We parse fetched_at (stored as a string by the producer) into a proper
-- DateTime here so downstream models don't have to deal with it.
-- Filters out any rows where price is zero or null — those are bad polls.

{{ config(materialized='view') }}

with source as (

    select
        ticker,
        name,
        price,
        volume,
        market_cap,
        currency,
        exchange,
        -- fetched_at comes in as ISO-8601 from Python's datetime.isoformat()
        -- e.g. "2026-04-21T10:15:00+03:00"
        -- ClickHouse's parseDateTime64BestEffort handles timezone offsets cleanly
        parseDateTime64BestEffort(fetched_at) as fetched_at,
        toDate(parseDateTime64BestEffort(fetched_at)) as trade_date,
        inserted_at

    from {{ source('nse_raw', 'raw_ticks') }}

    where price > 0
      and ticker != ''

),

deduped as (

    -- In case the producer sent the same ticker twice in the same second
    -- (e.g. after a restart), we keep only the latest row per ticker + second
    select *
    from source
    qualify row_number() over (
        partition by ticker, toStartOfMinute(fetched_at)
        order by inserted_at desc
    ) = 1

)

select * from deduped
