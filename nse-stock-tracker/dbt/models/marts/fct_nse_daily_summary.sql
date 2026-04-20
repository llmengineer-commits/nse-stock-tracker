-- models/marts/fct_nse_daily_summary.sql
--
-- One row per ticker per trading day.
-- This is the main table powering the Looker Studio dashboard.
-- Runs after market close via Kestra (15:35 EAT, Mon–Fri).
--
-- Columns:
--   trade_date     — the NSE trading day
--   ticker         — NSE ticker symbol
--   open_price     — first price of the day
--   high_price     — intraday high
--   low_price      — intraday low
--   close_price    — last price before 15:30 close
--   price_change   — absolute change (close - open), in KES
--   pct_change     — percentage change
--   total_ticks    — how many polls we captured that day (data quality proxy)

{{
  config(
    materialized = 'table',
    order_by     = ['trade_date', 'ticker'],
    partition_by = 'toYYYYMM(trade_date)',
    engine       = 'MergeTree()',
  )
}}

with daily_agg as (

    -- Aggregate the raw tick staging view to daily granularity
    select
        trade_date,
        ticker,

        -- First and last price of the day as open/close proxies
        argMin(price, fetched_at)  as open_price,
        argMax(price, fetched_at)  as close_price,
        max(price)                 as high_price,
        min(price)                 as low_price,
        count(*)                   as total_ticks

    from {{ ref('stg_nse_ticks') }}
    group by trade_date, ticker

),

enriched as (

    select
        trade_date,
        ticker,
        open_price,
        high_price,
        low_price,
        close_price,
        round(close_price - open_price, 2)                         as price_change,
        round((close_price - open_price) / open_price * 100, 2)   as pct_change,
        total_ticks,
        now()                                                       as updated_at

    from daily_agg

    -- Safety: don't include rows where open is zero (would cause division error)
    where open_price > 0

)

select * from enriched
order by trade_date desc, pct_change desc
