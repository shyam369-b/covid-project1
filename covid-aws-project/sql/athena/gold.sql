
-- Athena: Gold registration
CREATE DATABASE IF NOT EXISTS covid_gold_db;

-- Dimensions
DROP TABLE IF EXISTS covid_gold_db.dim_date;
CREATE EXTERNAL TABLE IF NOT EXISTS covid_gold_db.dim_date (
  date_id int,
  full_date date,
  year int,
  month int,
  day int,
  dow int,
  is_weekend boolean
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/covid/gold/dim_date/';

DROP TABLE IF EXISTS covid_gold_db.dim_state;
CREATE EXTERNAL TABLE IF NOT EXISTS covid_gold_db.dim_state (
  state_code string,
  state_name string
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/covid/gold/dim_state/';

-- Facts
DROP TABLE IF EXISTS covid_gold_db.fact_cases_state_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS covid_gold_db.fact_cases_state_daily (
  date_id int,
  state_code string,
  cases_cum bigint,
  deaths_cum bigint,
  new_cases int,
  new_deaths int
)
PARTITIONED BY (state_code string)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/covid/gold/fact_cases_state_daily/';
MSCK REPAIR TABLE covid_gold_db.fact_cases_state_daily;

DROP TABLE IF EXISTS covid_gold_db.fact_testing_state_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS covid_gold_db.fact_testing_state_daily (
  date_id int,
  state_code string,
  tests_total_cum bigint,
  tests_pos_cum bigint,
  tests_neg_cum bigint,
  new_tests int,
  positivity_rate double
)
PARTITIONED BY (state_code string)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/covid/gold/fact_testing_state_daily/';
MSCK REPAIR TABLE covid_gold_db.fact_testing_state_daily;

-- Sample analytics (adjust date/state as needed)
-- Top 5 states by new cases on a given date
-- SELECT s.state_name, f.new_cases
-- FROM covid_gold_db.fact_cases_state_daily f
-- JOIN covid_gold_db.dim_state s ON s.state_code = f.state_code
-- WHERE f.date_id = 20200515
-- ORDER BY f.new_cases DESC
-- LIMIT 5;

-- Positivity trend for NY
-- SELECT d.full_date,
--        (t.tests_pos_cum::double / NULLIF(t.tests_total_cum, 0)) AS positivity_rate
-- FROM covid_gold_db.fact_testing_state_daily t
-- JOIN covid_gold_db.dim_date d ON d.date_id = t.date_id
-- WHERE t.state_code = 'NY'
-- ORDER BY d.full_date;

-- Daily cases vs tests for CA
-- SELECT d.full_date,
--        c.new_cases,
--        t.new_tests,
--        (t.tests_pos_cum::double / NULLIF(t.tests_total_cum, 0)) AS positivity_rate
-- FROM covid_gold_db.fact_cases_state_daily c
-- JOIN covid_gold_db.fact_testing_state_daily t
--   ON t.date_id = c.date_id AND t.state_code = c.state_code
-- JOIN covid_gold_db.dim_date d ON d.date_id = c.date_id
-- WHERE c.state_code = 'CA'
-- ORDER BY d.full_date;
