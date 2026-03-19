with trades as (
    select * from {{ ref('stg_trades') }}
),

ohlcv as (
    select
        symbol,
        timestamp_trunc(traded_at, hour)            as hour,
        count(*)                                    as trade_count,
        round(sum(quantity), 6)                     as volume,
        round(sum(trade_value), 6)                  as turnover,
        round(min(price), 6)                        as low,
        round(max(price), 6)                        as high,
        round(
            sum(price * quantity) / sum(quantity), 6
        )                                           as vwap
    from trades
    group by symbol, hour
)

select * from ohlcv
order by symbol, hour desc