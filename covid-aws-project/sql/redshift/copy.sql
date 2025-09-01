# streamlit_covid_dashboard.py
import streamlit as st
import boto3
import pandas as pd
import io
import time
import matplotlib.pyplot as plt

# -----------------------------
# Configuration
# -----------------------------
ATHENA_DB = {
    "bronze": "covid_bronze_db",
    "silver": "covid_silver_db",
    "gold": "covid_gold_db"
}

S3_BUCKET = "my-covid19-analytics-bucket"
S3_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"

athena = boto3.client('athena', region_name='us-east-2')
s3 = boto3.client('s3')

# -----------------------------
# Helper functions
# -----------------------------
def run_athena_query(query, database='default', output=S3_OUTPUT):
    """Run Athena query and wait for completion"""
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output}
    )
    qid = response['QueryExecutionId']
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)
    if state != 'SUCCEEDED':
        raise Exception(f"Athena query failed: {state}")
    return qid

def fetch_athena_results(query, database='covid_gold_db'):
    """Run query and fetch results as Pandas DataFrame"""
    qid = run_athena_query(query, database=database, output=S3_OUTPUT+"gold/")
    # Athena writes results as CSV in S3
    prefix = f"gold/{qid}/"
    while True:
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        if 'Contents' in resp and len(resp['Contents']) > 0:
            break
        time.sleep(2)
    key = resp['Contents'][0]['Key']
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df

# -----------------------------
# Streamlit UI
# -----------------------------
st.title("📊 COVID-19 Analytics Dashboard (Gold Layer)")
st.write("Click the button to refresh data and run the full Athena pipeline.")

if st.button("Refresh Data & Run Pipeline"):
    try:
        st.info("Running Athena pipeline... please wait ⏳")

        # -----------------------------
        # 1️⃣ Bronze Layer
        # -----------------------------
        run_athena_query(f"CREATE DATABASE IF NOT EXISTS {ATHENA_DB['bronze']};")

        run_athena_query("""
        CREATE EXTERNAL TABLE IF NOT EXISTS covid_bronze_db.raw_cases (
            date STRING,
            state STRING,
            cases STRING,
            deaths STRING,
            new_cases STRING,
            new_deaths STRING
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
            "separatorChar" = ",",
            "skip.header.line.count" = "1"
        )
        LOCATION 's3://my-covid19-analytics-bucket/covid/bronze/covid_tracking/';
        """, database=ATHENA_DB['bronze'])

        run_athena_query("""
        CREATE EXTERNAL TABLE IF NOT EXISTS covid_bronze_db.raw_states (
            state_code STRING,
            state_name STRING
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
            "separatorChar" = ",",
            "skip.header.line.count" = "1"
        )
        LOCATION 's3://my-covid19-analytics-bucket/covid/bronze/nytimes/';
        """, database=ATHENA_DB['bronze'])

        run_athena_query("""
        CREATE EXTERNAL TABLE IF NOT EXISTS covid_bronze_db.raw_state_abv (
            state_code STRING,
            abbreviation STRING
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
            "separatorChar" = ",",
            "skip.header.line.count" = "1"
        )
        LOCATION 's3://my-covid19-analytics-bucket/covid/bronze/static/';
        """, database=ATHENA_DB['bronze'])

        # -----------------------------
        # 2️⃣ Silver Layer
        # -----------------------------
        run_athena_query(f"CREATE DATABASE IF NOT EXISTS {ATHENA_DB['silver']};")

        run_athena_query(f"""
        CREATE OR REPLACE TABLE {ATHENA_DB['silver']}.silver_cases AS
        SELECT
            date,
            state,
            TRY_CAST(cases AS BIGINT) AS cases,
            TRY_CAST(deaths AS BIGINT) AS deaths,
            TRY_CAST(new_cases AS BIGINT) AS new_cases,
            TRY_CAST(new_deaths AS BIGINT) AS new_deaths
        FROM {ATHENA_DB['bronze']}.raw_cases;
        """, database=ATHENA_DB['silver'])

        run_athena_query(f"""
        CREATE OR REPLACE TABLE {ATHENA_DB['silver']}.silver_states AS
        SELECT *
        FROM {ATHENA_DB['bronze']}.raw_states;
        """, database=ATHENA_DB['silver'])

        # -----------------------------
        # 3️⃣ Gold Layer
        # -----------------------------
        run_athena_query(f"CREATE DATABASE IF NOT EXISTS {ATHENA_DB['gold']};")

        run_athena_query(f"""
        CREATE OR REPLACE TABLE {ATHENA_DB['gold']}.fact_cases_state_daily AS
        SELECT c.date, s.state_name, c.cases, c.deaths, c.new_cases, c.new_deaths
        FROM {ATHENA_DB['silver']}.silver_cases c
        LEFT JOIN {ATHENA_DB['silver']}.silver_states s
        ON TRIM(c.state) = TRIM(s.state_code);
        """, database=ATHENA_DB['gold'])

        # -----------------------------
        # 4️⃣ Fetch Gold Data
        # -----------------------------
        df_gold = fetch_athena_results("SELECT * FROM covid_gold_db.fact_cases_state_daily LIMIT 1000;")
        if df_gold.empty:
            st.warning("No data found in Gold table! Check Bronze CSVs in S3.")
        else:
            st.success("Data refreshed successfully ✅")
            st.dataframe(df_gold)

            # -----------------------------
            # 5️⃣ Visualizations
            # -----------------------------
            st.subheader("Daily New Cases Trend")
            daily_cases = df_gold.groupby('date')['new_cases'].sum().reset_index()
            plt.figure(figsize=(10,5))
            plt.plot(daily_cases['date'], daily_cases['new_cases'], marker='o')
            plt.xticks(rotation=45)
            plt.ylabel("New Cases")
            st.pyplot(plt)

            st.subheader("Top 10 States by Total Cases")
            top_states = df_gold.groupby('state_name')['cases'].sum().sort_values(ascending=False).head(10)
            st.bar_chart(top_states)

    except Exception as e:
        st.error(f"❌ Pipeline failed: {e}")
