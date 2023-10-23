CREATE TABLE ecommerce_stats(
  ts TIMESTAMP,
  country SYMBOL capacity 256 CACHE,
  category SYMBOL capacity 256 CACHE,
  visits LONG,
  unique_visitors LONG,
  sales DOUBLE,
  number_of_products INT
) timestamp (ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, country, category);

truncate table ecommerce_stats;

WITH countries AS (
  select 'UK' as country, rnd_long(500, 520, 0) as visits_multiplier,  rnd_long(100, 300, 0) as unique_multiplier  from long_sequence(1, 128349234,4327897)
  UNION
  select 'ES' as country, rnd_long(300, 310, 0) as visits_multiplier,  rnd_long(40, 125, 0) as unique_multiplier from long_sequence(1, 128349234,4327897)
  UNION
  select 'FR' as country, rnd_long(400, 480, 0) as visits_multiplier,  rnd_long(130, 240, 0) as unique_multiplier from long_sequence(1, 128349234,4327897)
  UNION
  select 'DE' as country, rnd_long(420, 500, 0) as visits_multiplier,  rnd_long(80, 280, 0) as unique_multiplier from long_sequence(1, 128349234,4327897)
  UNION
  select 'IT' as country, rnd_long(350, 400, 0) as visits_multiplier,  rnd_long(60, 150, 0) as unique_multiplier from long_sequence(1, 128349234,4327897)
),
categories AS (
  select 'HOME' as category, 67.53 as avg_unit_price, rnd_int(1,6,0) as product_multiplier from long_sequence(1, 128349234,4327897)
  UNION
  select 'KITCHEN' as category, 26.32 as avg_unit_price, rnd_int(1,6,0) as product_multiplier from long_sequence(1, 128349234,4327897)
  UNION
  select 'BATHROOM' as category, 23.67 as avg_unit_price, rnd_int(1,6,0) as product_multiplier from long_sequence(1, 128349234,4327897)
),
timeline as (
  SELECT x as rownum, timestamp_sequence(
            to_timestamp('2022-01-01', 'yyyy-MM-dd'),
            86400000000L) as ts,
            (x / 100.0)  * ( 1 + 0.5 * cos(x*6.28/90)) as visits,
            (x / 100.0)  * ( 1 + 0.2 * cos(x*6.28/90)) as unique_visitors
FROM long_sequence(365, 128349234,4327897)
)

INSERT INTO ecommerce_stats
select ts, country, category,
      visits * visits_multiplier as visits, unique_visitors * unique_multiplier as unique_visitors,
      avg_unit_price * product_multiplier as sales, product_multiplier as number_of_products
 from countries cross join categories cross join timeline;


------ optionally if we want extra ln_timestamp column as long
alter table ecommerce_sample_test_global ADD COLUMN ln_timestamp LONG;
update ecommerce_sample_test_global SET ln_timestamp = ts;
