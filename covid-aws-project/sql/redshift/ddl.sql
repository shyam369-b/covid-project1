
-- Redshift star schema
CREATE SCHEMA IF NOT EXISTS covid_gold;

CREATE TABLE IF NOT EXISTS covid_gold.dim_date (
  date_id int PRIMARY KEY,
  full_date date,
  year smallint,
  month smallint,
  day smallint,
  dow smallint,
  is_weekend boolean
) SORTKEY(date_id);

CREATE TABLE IF NOT EXISTS covid_gold.dim_state (
  state_code varchar(2) PRIMARY KEY,
  state_name varchar(64)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS covid_gold.fact_cases_state_daily (
  date_id int NOT NULL REFERENCES covid_gold.dim_date(date_id),
  state_code varchar(2) NOT NULL REFERENCES covid_gold.dim_state(state_code),
  cases_cum bigint,
  deaths_cum bigint,
  new_cases int,
  new_deaths int
) DISTKEY(state_code) SORTKEY(date_id, state_code);

CREATE TABLE IF NOT EXISTS covid_gold.fact_testing_state_daily (
  date_id int NOT NULL REFERENCES covid_gold.dim_date(date_id),
  state_code varchar(2) NOT NULL REFERENCES covid_gold.dim_state(state_code),
  tests_total_cum bigint,
  tests_pos_cum bigint,
  tests_neg_cum bigint,
  new_tests int,
  positivity_rate double precision
) DISTKEY(state_code) SORTKEY(date_id, state_code);
