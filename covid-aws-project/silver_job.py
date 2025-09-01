import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# -------------------
# Glue boilerplate
# -------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
BUCKET = args["BUCKET"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------
# Input locations (Bronze)
# -------------------
bronze_cases = f"s3://{BUCKET}/covid/bronze/nytimes/us_states.csv"
bronze_tests = f"s3://{BUCKET}/covid/bronze/covid_tracking/states_daily.csv"
states_lookup = f"s3://{BUCKET}/covid/bronze/static/states_abv.csv"

# -------------------
# Load data
# -------------------
df_cases = spark.read.option("header", True).csv(bronze_cases)
df_tests = spark.read.option("header", True).csv(bronze_tests)
df_lookup = spark.read.option("header", True).csv(states_lookup)

# -------------------
# Clean + transform NYTimes cases
# -------------------
cases_std = (
    df_cases
    .withColumn("date_id", F.regexp_replace("date", "-", ""))   # e.g. 2020-03-15 -> 20200315
    .withColumnRenamed("state", "state_name")
    .withColumn("cases", F.col("cases").cast("bigint"))
    .withColumn("deaths", F.col("deaths").cast("bigint"))
    # Correct join: state_name (NYT) ↔ name (lookup)
    .join(df_lookup, F.col("state_name") == F.col("name"), how="left")
    .withColumnRenamed("abbr", "state_code")
    .withColumn("year", F.substring("date_id", 1, 4))
    .withColumn("month", F.substring("date_id", 5, 2))
    .withColumn("day", F.substring("date_id", 7, 2))
    .select("date_id", "state_code", "cases", "deaths", "year", "month", "day")
)


# -------------------
# Clean + transform COVID Tracking Project tests
# -------------------
tests_std = (
    df_tests
    .withColumn("date_id", F.col("date").cast("string"))
    .withColumn("tests_total", F.col("totalTestResults").cast("bigint"))
    .withColumn("tests_positive", F.col("positive").cast("bigint"))
    .withColumn("tests_negative", F.col("negative").cast("bigint"))
    .withColumnRenamed("state", "state_code")
    .withColumn("year", F.substring("date_id", 1, 4))
    .withColumn("month", F.substring("date_id", 5, 2))
    .withColumn("day", F.substring("date_id", 7, 2))
    .select("date_id", "state_code", "tests_total", "tests_positive", "tests_negative", "year", "month", "day")
)

# -------------------
# Write out to Silver
# -------------------
silver_cases_path = f"s3://{BUCKET}/covid/silver/cases_standardized/"
silver_tests_path = f"s3://{BUCKET}/covid/silver/testing_standardized/"

(
    cases_std.write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(silver_cases_path)
)

(
    tests_std.write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(silver_tests_path)
)

job.commit()
