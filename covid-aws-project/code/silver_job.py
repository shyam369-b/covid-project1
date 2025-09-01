
from pyspark.sql import SparkSession, functions as F
import os, argparse

# -------- Args & env --------
parser = argparse.ArgumentParser()
parser.add_argument("--BUCKET", type=str, default=os.getenv("BUCKET"))
args, _ = parser.parse_known_args()
if not args.BUCKET:
    raise SystemExit("Please pass --BUCKET YOUR_BUCKET or set env BUCKET")
BUCKET = args.BUCKET

BRONZE = f"s3://{BUCKET}/covid/bronze"
SILVER = f"s3://{BUCKET}/covid/silver"

spark = (SparkSession.builder
         .appName("covid-minimal-silver")
         .getOrCreate())

upper_trim = lambda c: F.upper(F.trim(F.col(c)))

# --- Lookups ---
states = (spark.read.option("header", True)
          .csv(f"{BRONZE}/static/states_abv.csv")
          .select(upper_trim("abbr").alias("state_code"),
                  F.initcap("name").alias("state_name"))
         )

# --- Cases (NYT state file: date,state,cases,deaths) ---
cases_raw = spark.read.option("header", True).csv(f"{BRONZE}/nytimes/us_states.csv")

cases_std = (cases_raw
    .withColumn("full_date", F.to_date("date"))
    .withColumn("state_name_raw", F.initcap(F.col("state")))
    .join(states, states.state_name == F.col("state_name_raw"), "left")
    .withColumn("cases_cum", F.col("cases").cast("long"))
    .withColumn("deaths_cum", F.col("deaths").cast("long"))
    .withColumn("year", F.year("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day", F.dayofmonth("full_date"))
    .select("full_date", "state_code", "state_name", "cases_cum", "deaths_cum",
            "year", "month", "day")
    .dropna(subset=["full_date", "state_code"])
)

(cases_std.write.mode("overwrite")
  .partitionBy("state_code", "year", "month", "day")
  .parquet(f"{SILVER}/cases_standardized")
)

# --- Testing (COVID Tracking: date(yyyymmdd int), state(code), positive, negative, totalTestResults) ---
tests_raw = spark.read.option("header", True).csv(f"{BRONZE}/covid_tracking/states_daily.csv")

tests_std = (tests_raw
    .withColumn("full_date", F.to_date(F.col("date").cast("string"), "yyyyMMdd"))
    .withColumn("state_code", upper_trim("state"))
    .join(states.select("state_code", "state_name"), "state_code", "left")
    .withColumn("tests_total_cum", F.col("totalTestResults").cast("long"))
    .withColumn("tests_pos_cum", F.col("positive").cast("long"))
    .withColumn("tests_neg_cum", F.col("negative").cast("long"))
    .withColumn("year", F.year("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day", F.dayofmonth("full_date"))
    .select("full_date", "state_code", "state_name",
            "tests_total_cum", "tests_pos_cum", "tests_neg_cum",
            "year", "month", "day")
    .dropna(subset=["full_date", "state_code"])
)

(tests_std.write.mode("overwrite")
  .partitionBy("state_code", "year", "month", "day")
  .parquet(f"{SILVER}/testing_standardized")
)

print("Silver complete.")
