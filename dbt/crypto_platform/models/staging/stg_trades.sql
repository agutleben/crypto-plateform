with source as (
    select *
    from `{{ var('gcp_project') }}.{{ var('bq_dataset_raw') }}.trades`
),

cleaned as (
    select
        symbol,
        price,
        quantity,
        price * quantity                             as trade_value,
        is_buyer_market_maker,
        timestamp_millis(trade_time)                as traded_at,
        timestamp_millis(event_time)                as event_at,
        date(timestamp_millis(trade_time))          as trade_date
    from source
    where price > 0
      and quantity > 0
)

select * from cleaned