

  create or replace view `crypto-platform-dev-490610`.`crypto_mart`.`stg_metrics`
  OPTIONS()
  as with source as (
    select *
    from `crypto-platform-dev-490610.crypto_raw.metrics`
),

cleaned as (
    select
        symbol,
        round(vwap, 6)                              as vwap,
        round(volume, 6)                            as volume,
        trade_count,
        round(price_min, 6)                         as price_min,
        round(price_max, 6)                         as price_max,
        round(price_open, 6)                        as price_open,
        round(price_close, 6)                       as price_close,
        round(price_change_pct, 4)                  as price_change_pct,
        round(price_max - price_min, 6)             as price_range,
        window_start,
        window_end
    from source
    where vwap > 0
)

select * from cleaned;

