
from pyspark.sql import SparkSession, functions as F, window as W

import os, argparse

parser = argparse.ArgumentParser()
parser.add_argument("--BUCKET", type=str, default=os.getenv("BUCKET"))
args, _ = parser.parse_known_args()
if not args.BUCKET:
    raise SystemExit("Please pass --BUCKET YOUR_BUCKET or set env BUCKET")
BUCKET = args.BUCKET

SILVER = f"s3://{BUCKET}/covid/silver"
GOLD   = f"s3://{BUCKET}/covid/gold"

spark = (SparkSession.builder
         .appName("covid-minimal-gold")
         .getOrCreate())

cases = spark.read.parquet(f"{SILVER}/cases_standardized")
tests = spark.read.parquet(f"{SILVER}/testing_standardized")

# dim_date
all_dates = (cases.select("full_date").union(tests.select("full_date")).dropDuplicates())
dim_date = (all_dates
  .withColumn("date_id", F.date_format("full_date", "yyyyMMdd").cast("int"))
  .withColumn("year", F.year("full_date"))
  .withColumn("month", F.month("full_date"))
  .withColumn("day", F.dayofmonth("full_date"))
  .withColumn("dow", F.date_format("full_date", "u").cast("int"))
  .withColumn("is_weekend", F.col("dow").isin([6,7]))
)
dim_date.write.mode("overwrite").parquet(f"{GOLD}/dim_date")

# dim_state
dim_state = cases.select("state_code", "state_name").dropDuplicates()
dim_state.write.mode("overwrite").parquet(f"{GOLD}/dim_state")

# fact_cases_state_daily
w = W.Window.partitionBy("state_code").orderBy("full_date")
fact_cases = (cases
  .withColumn("date_id", F.date_format("full_date", "yyyyMMdd").cast("int"))
  .withColumn("new_cases",  F.greatest(F.col("cases_cum")  - F.lag("cases_cum").over(w),  F.lit(0)))
  .withColumn("new_deaths", F.greatest(F.col("deaths_cum") - F.lag("deaths_cum").over(w), F.lit(0)))
  .select("date_id", "state_code", "cases_cum", "deaths_cum", "new_cases", "new_deaths")
)
fact_cases.write.mode("overwrite").partitionBy("state_code").parquet(f"{GOLD}/fact_cases_state_daily")

# fact_testing_state_daily
w2 = W.Window.partitionBy("state_code").orderBy("full_date")
fact_tests = (tests
  .withColumn("date_id", F.date_format("full_date", "yyyyMMdd").cast("int"))
  .withColumn("new_tests", F.greatest(F.col("tests_total_cum") - F.lag("tests_total_cum").over(w2), F.lit(0)))
  .withColumn("positivity_rate", F.when(F.col("tests_total_cum") > 0,
                                       (F.col("tests_pos_cum") / F.col("tests_total_cum")).cast("double")))
  .select("date_id", "state_code", "tests_total_cum", "tests_pos_cum", "tests_neg_cum", "new_tests", "positivity_rate")
)
fact_tests.write.mode("overwrite").partitionBy("state_code").parquet(f"{GOLD}/fact_testing_state_daily")

print("Gold complete.")
