import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
# Input locations (Silver)
# -------------------
silver_cases = f"s3://{BUCKET}/covid/silver/cases_standardized/"
silver_tests = f"s3://{BUCKET}/covid/silver/testing_standardized/"

# -------------------
# Load Silver data
# -------------------
df_cases = spark.read.parquet(silver_cases)
df_tests = spark.read.parquet(silver_tests)

# -------------------
# Dimension: Date
# -------------------
dim_date = (
    df_cases.select("date_id", "year", "month", "day")
    .distinct()
    .orderBy("date_id")
)

dim_date_path = f"s3://{BUCKET}/covid/gold/dim_date/"
dim_date.write.mode("overwrite").parquet(dim_date_path)

# -------------------
# Dimension: State
# -------------------
dim_state = (
    df_cases.select("state_code")
    .distinct()
    .orderBy("state_code")
)

dim_state_path = f"s3://{BUCKET}/covid/gold/dim_state/"
dim_state.write.mode("overwrite").parquet(dim_state_path)

# -------------------
# Fact: Cases (daily deltas)
# -------------------
windowSpec = Window.partitionBy("state_code").orderBy("date_id")

fact_cases = (
    df_cases
    .withColumn("cases_prev", F.lag("cases").over(windowSpec))
    .withColumn("deaths_prev", F.lag("deaths").over(windowSpec))
    .withColumn("new_cases", (F.col("cases") - F.col("cases_prev")).cast("bigint"))
    .withColumn("new_deaths", (F.col("deaths") - F.col("deaths_prev")).cast("bigint"))
    .fillna({"new_cases": 0, "new_deaths": 0})
    .select("date_id", "state_code", "cases", "deaths", "new_cases", "new_deaths")
)

fact_cases_path = f"s3://{BUCKET}/covid/gold/fact_cases_state_daily/"
fact_cases.write.mode("overwrite").parquet(fact_cases_path)

# -------------------
# Fact: Testing (daily deltas + positivity rate)
# -------------------
fact_tests = (
    df_tests
    .withColumn("tests_prev", F.lag("tests_total").over(windowSpec))
    .withColumn("new_tests", (F.col("tests_total") - F.col("tests_prev")).cast("bigint"))
    .fillna({"new_tests": 0})
    .withColumn("positivity_rate", F.when(F.col("tests_total") > 0,
                                          F.round(F.col("tests_positive") / F.col("tests_total") * 100, 2))
                .otherwise(None))
    .select("date_id", "state_code", "tests_total", "tests_positive", "tests_negative", "new_tests", "positivity_rate")
)

fact_tests_path = f"s3://{BUCKET}/covid/gold/fact_testing_state_daily/"
fact_tests.write.mode("overwrite").parquet(fact_tests_path)

# -------------------
# Commit Glue job
# -------------------
job.commit()
