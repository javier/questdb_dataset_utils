select count(*) from ecommerce_sample_test;

select * from ecommerce_sample_test limit -20;


CREATE TABLE IF NOT EXISTS  base_timeline_1min_365(rownum int, ts timestamp) timestamp(ts) partition by day WAL DEDUP UPSERT KEYS(ts);
truncate table base_timeline_1min_365;
INSERT INTO base_timeline_1min_365
  SELECT x as rownum, timestamp_sequence(
            to_timestamp('2031-01-01', 'yyyy-MM-dd'),
            60000000L) as ts -- 60000000L --86400000000L
FROM long_sequence(2102400, 128349234,4327897); --525600L one year


CREATE TABLE IF NOT EXISTS  base_categories(category SYMBOL, avg_unit_price double);
truncate table base_categories;
WITH categories AS (
  select 'WOMEN' as category, 57.45 as avg_unit_price
  UNION
  select 'MEN' as category, 46.34 as avg_unit_price
  UNION
  select 'KIDS' as category, 49.75 as avg_unit_price
  UNION
  select 'HOME' as category, 67.53 as avg_unit_price
  UNION
  select 'KITCHEN' as category, 26.32 as avg_unit_price
  UNION
  select 'BATHROOM' as category, 23.67 as avg_unit_price
)
INSERT INTO base_categories
SELECT * from categories;

CREATE TABLE IF NOT EXISTS  'ecommerce_sample' (
  ts TIMESTAMP,
  country SYMBOL capacity 256 CACHE,
  category SYMBOL capacity 256 CACHE,
  visits LONG,
  unique_visitors LONG,
  avg_unit_price DOUBLE,
  sales DOUBLE
) timestamp (ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, country,category);
truncate table ecommerce_sample;

WITH
base_properties AS (
SELECT ts,CAST('UK' as Symbol) as country,
            rnd_long(500, 520, 0)  * rownum   * (1 + 0.2 * cos(rownum*6.28/43800)) as visits,
            rnd_long(100, 300, 0)  * rownum  * (1 + 0.5 * cos(rownum*6.28/43800))as unique_visitors
FROM base_timeline_1min_365
UNION
SELECT ts,CAST('ES'  as Symbol) as country,
            rnd_long(300, 310, 0)  * rownum   * (1 + 0.2 * cos(rownum*6.28/43800)) as visits,
            rnd_long(40, 125, 0)  * rownum  * (1 + 0.5 * cos(rownum*6.28/43800))as unique_visitors
FROM base_timeline_1min_365
UNION
SELECT ts,CAST('FR' as Symbol) as country,
            rnd_long(400, 480, 0)  * rownum   * (1 + 0.2 * cos(rownum*6.28/43800)) as visits,
            rnd_long(130, 240, 0)  * rownum  * (1 + 0.5 * cos(rownum*6.28/43800))as unique_visitors
FROM base_timeline_1min_365
UNION
SELECT ts,CAST('DE' as Symbol) as country,
            rnd_long(420, 500, 0)  * rownum   * (1 + 0.2 * cos(rownum*6.28/43800)) as visits,
            rnd_long(80, 280, 0)  * rownum  * (1 + 0.5 * cos(rownum*6.28/43800))as unique_visitors
FROM base_timeline_1min_365
UNION
SELECT ts,CAST('IT' as Symbol) as country,
            rnd_long(350, 400, 0)  * rownum   * (1 + 0.2 * cos(rownum*6.28/43800)) as visits,
            rnd_long(60, 150, 0)  * rownum  * (1 + 0.5 * cos(rownum*6.28/43800))as unique_visitors
FROM base_timeline_1min_365
),
 properties_with_prices AS (
select ts, country, CAST(visits as LONG) as visits, CAST(unique_visitors as LONG) as unique_visitors,
category, avg_unit_price,
unique_visitors * rnd_int(3,4,0) / 100 * rnd_int(1,6,0) as number_of_products
from base_properties cross join base_categories
),
ecommerce_dataset AS (
select ts, country, category, visits, unique_visitors, avg_unit_price, number_of_products * avg_unit_price as sales
from properties_with_prices
)
 INSERT INTO ecommerce_sample
 SELECT * from ecommerce_dataset;

select count() from ecommerce_sample;

alter table ecommerce_sample_test DEDUP disable;
