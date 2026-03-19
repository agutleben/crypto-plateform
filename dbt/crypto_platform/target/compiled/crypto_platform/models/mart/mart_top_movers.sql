with latest_metrics as (
    select
        symbol,
        vwap,
        volume,
        trade_count,
        price_change_pct,
        price_min,
        price_max,
        price_range,
        window_start,
        window_end,
        row_number() over (
            partition by symbol
            order by window_end desc
        ) as rn
    from `crypto-platform-dev-490610`.`crypto_mart`.`stg_metrics`
),

top_movers as (
    select
        symbol,
        vwap,
        volume,
        trade_count,
        price_change_pct,
        price_min,
        price_max,
        price_range,
        window_start,
        window_end,
        abs(price_change_pct)                       as abs_change_pct,
        case
            when price_change_pct > 0 then 'UP'
            when price_change_pct < 0 then 'DOWN'
            else 'FLAT'
        end                                         as direction
    from latest_metrics
    where rn = 1
)

select * from top_movers
order by abs_change_pct desc