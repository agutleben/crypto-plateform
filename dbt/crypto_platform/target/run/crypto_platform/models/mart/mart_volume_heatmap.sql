
  
    

    create or replace table `crypto-platform-dev-490610`.`crypto_mart`.`mart_volume_heatmap`
      
    
    

    OPTIONS()
    as (
      with trades as (
    select * from `crypto-platform-dev-490610`.`crypto_mart`.`stg_trades`
),

heatmap as (
    select
        symbol,
        extract(hour from traded_at)                as hour_of_day,
        extract(dayofweek from traded_at)           as day_of_week,
        count(*)                                    as trade_count,
        round(sum(quantity), 4)                     as volume,
        round(avg(price), 6)                        as avg_price
    from trades
    group by symbol, hour_of_day, day_of_week
)

select * from heatmap
order by symbol, day_of_week, hour_of_day
    );
  