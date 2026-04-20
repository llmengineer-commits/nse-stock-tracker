-- models/marts/fct_nse_top_movers.sql
--
-- Top 5 gainers and top 5 losers for each trading day.
-- Small table, fast to query — Looker Studio uses this for the
-- "Today's movers" scorecards and bar chart on the main dashboard.

{{
  config(
    materialized = 'table',
    order_by     = ['trade_date', 'direction', 'rank'],
    engine       = 'MergeTree()',
  )
}}

with ranked as (

    select
        trade_date,
        ticker,
        close_price,
        pct_change,

        -- Rank within gainers (pct_change > 0, highest first)
        row_number() over (
            partition by trade_date
            order by pct_change desc
        ) as gainer_rank,

        -- Rank within losers (pct_change < 0, lowest first)
        row_number() over (
            partition by trade_date
            order by pct_change asc
        ) as loser_rank

    from {{ ref('fct_nse_daily_summary') }}

    -- Only include stocks that moved — flat lines aren't interesting
    where pct_change != 0
      and total_ticks >= 3   -- needs at least 3 polls to be a reliable signal

),

gainers as (

    select
        trade_date,
        'gainer'    as direction,
        gainer_rank as rank,
        ticker,
        close_price,
        pct_change
    from ranked
    where pct_change > 0
      and gainer_rank <= 5

),

losers as (

    select
        trade_date,
        'loser'    as direction,
        loser_rank as rank,
        ticker,
        close_price,
        pct_change
    from ranked
    where pct_change < 0
      and loser_rank <= 5

)

select * from gainers
union all
select * from losers

order by trade_date desc, direction, rank
