with ranked as (
    select
        symbol,
        vwap,
        volume,
        trade_count,
        price_min,
        price_max,
        price_open,
        price_close,
        price_range,
        window_start,
        window_end,
        row_number() over (
            partition by symbol
            order by window_end desc
        ) as rn
    from {{ ref('stg_metrics') }}
),

current_window as (
    select * from ranked where rn = 1
),

previous_window as (
    select * from ranked where rn = 5
),

top_movers as (
    select
        c.symbol,
        c.vwap,
        c.volume,
        c.trade_count,
        c.price_min,
        c.price_max,
        c.price_range,
        c.window_start,
        c.window_end,
        round(
            (c.price_close - p.price_open) / p.price_open * 100,
            4
        )                                               as price_change_pct,
        abs(round(
            (c.price_close - p.price_open) / p.price_open * 100,
            4
        ))                                              as abs_change_pct,
        case
            when c.price_close > p.price_open then 'UP'
            when c.price_close < p.price_open then 'DOWN'
            else 'FLAT'
        end                                             as direction
    from current_window c
    join previous_window p using (symbol)
)

select * from top_movers
order by abs_change_pct desc