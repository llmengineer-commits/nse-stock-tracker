-- models/staging/stg_nse_ohlcv.sql
--
-- Thin cleanup layer over the 5-minute OHLCV aggregates from Flink.
-- Mostly casting + sanity checks — Flink already did the heavy aggregation.

{{ config(materialized='view') }}

with source as (

    select
        ticker,
        window_start,
        window_end,
        open_price,
        high_price,
        low_price,
        close_price,
        tick_count,
        toDate(window_start) as trade_date,
        inserted_at

    from {{ source('nse_raw', 'ohlcv_5min') }}

    -- A window with 0 ticks shouldn't exist, but Flink can occasionally
    -- emit empty windows during job restarts — filter them out
    where tick_count > 0
      and open_price  > 0
      and close_price > 0

)

select * from source
