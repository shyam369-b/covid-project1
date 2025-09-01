
# COVID-19 Analytics (AWS) — End-to-End Project

This repo packages the full pipeline defined in your document into a runnable project: **Bronze ➜ Silver ➜ Gold ➜ (optional) Redshift ➜ BI queries**.

> Replace all occurrences of `YOUR_BUCKET` and `YOUR_ACCOUNT_ID` with your values.

---

## Project layout

```
covid-aws-project/
├─ code/
│  ├─ silver_job.py         # Bronze -> Silver curation (PySpark)
│  └─ gold_job.py           # Silver -> Gold star schema (PySpark)
├─ sql/
│  ├─ athena/
│  │  ├─ silver.sql         # Create+repair Athena tables for Silver
│  │  └─ gold.sql           # Create Athena tables for Gold
│  └─ redshift/
│     ├─ ddl.sql            # Star schema in Redshift
│     └─ copy.sql           # COPY (Parquet) from S3 Gold to Redshift
├─ scripts/
│  ├─ 00_upload_code.sh     # Sync code & SQL to S3
│  ├─ 01_prepare_bronze.sh  # Put Bronze data & static lookups in your bucket
│  └─ 02_run_local_notes.md # Tips if you prefer local Spark testing
└─ data/
   └─ static/
      └─ states_abv.csv     # State abbrev ↔ name lookup
```

---

## Prerequisites

- AWS account with permissions for **S3, IAM, Glue or EMR, Athena**, and (optional) **Redshift**.
- **AWS CLI v2** configured (`aws configure`), and a default region set.
- A unique S3 bucket, e.g. `my-covid19-analytics-bucket` in your region.
- (Optional) A Redshift cluster or Redshift Serverless workgroup if you plan to COPY Gold tables.

> **Bucket layout this project uses**
>
> ```
> s3://YOUR_BUCKET/covid/
>   bronze/{nytimes|covid_tracking|static}/...
>   silver/{cases_standardized|testing_standardized}/...
>   gold/{dim_date|dim_state|fact_cases_state_daily|fact_testing_state_daily}/...
> ```

---

## Step-by-step runbook

### 1) Create / choose an S3 bucket (once)

```bash
aws s3 mb s3://YOUR_BUCKET
```

### 2) Upload code & SQL to S3 (once or when you change code)

```bash
cd covid-aws-project
export BUCKET=YOUR_BUCKET
bash scripts/00_upload_code.sh
```

This will sync:
- `code/` ➜ `s3://$BUCKET/code/`
- `sql/`  ➜ `s3://$BUCKET/sql/`
- `data/static/states_abv.csv` ➜ `s3://$BUCKET/covid/bronze/static/states_abv.csv`

### 3) Stage Bronze data into your bucket

The project expects three things in **Bronze**:
- `nytimes/us_states.csv` (cases, deaths by state, daily cumulative)
- `covid_tracking/states_daily.csv` (tests totals by state, daily cumulative)
- `static/states_abv.csv` (lookup; already uploaded by script above)

Run:

```bash
export BUCKET=YOUR_BUCKET
bash scripts/01_prepare_bronze.sh
```

The script shows `aws s3 ls` commands to help you locate the exact file names in the AWS COVID-19 public lake and copy them into your Bronze folders. (Names vary slightly; the script includes common patterns and examples.)

### 4) Run the **Silver** PySpark job (Bronze ➜ Silver)

**Option A — AWS Glue (recommended, serverless):**
1. In AWS Glue ➜ **Jobs** ➜ **Create job**:
   - Name: `covid-silver`
   - Type: Spark, **Glue 4.0** (Spark 3.x), Python 3.
   - Script location: `s3://YOUR_BUCKET/code/silver_job.py`
   - Job parameters (optional): `--BUCKET=YOUR_BUCKET`
2. Add **IAM role** with S3 read/write to your bucket.
3. **Run** the job and wait until status is `SUCCEEDED`.

**Option B — EMR cluster + spark-submit:**
```bash
spark-submit s3://YOUR_BUCKET/code/silver_job.py --BUCKET YOUR_BUCKET
```

**What you’ll get** under `s3://YOUR_BUCKET/covid/silver/`:
- `cases_standardized/` and `testing_standardized/` in **Parquet**, partitioned by `state_code/year/month/day`.

### 5) Create Athena tables for Silver (Glue Data Catalog)

Open **Athena** (set an output location in Settings once), then run:

```sql
-- File: sql/athena/silver.sql
-- Replace YOUR_BUCKET and run all statements
```

This registers `covid_silver_db.cases_standardized` and `covid_silver_db.testing_standardized`. Finish with `MSCK REPAIR TABLE ...` to load partitions.

### 6) Run the **Gold** PySpark job (Silver ➜ Gold)

Again via **Glue** or **EMR** (similar to Step 4), but point to:

- Script: `s3://YOUR_BUCKET/code/gold_job.py`
- Parameter: `--BUCKET=YOUR_BUCKET`

Outputs under `s3://YOUR_BUCKET/covid/gold/`:
- `dim_date/`, `dim_state/`, `fact_cases_state_daily/`, `fact_testing_state_daily/` (Parquet)

### 7) Create Athena tables for Gold

Run **`sql/athena/gold.sql`** in Athena (update bucket name).

### 8) (Optional) Publish to Redshift

- Run **`sql/redshift/ddl.sql`** in your Redshift endpoint to create schema & tables.
- Run **`sql/redshift/copy.sql`** to COPY Parquet from S3 Gold into Redshift.

### 9) BI / sanity queries

- Top 5 states by new cases on a specific date
- 7‑day positivity trend for a state
- Daily cases vs tests

All included in **`sql/athena/gold.sql`** comments and can be adapted for Redshift.

---

## Notes & tips

- Gold facts compute day-over-day deltas defensively (no negatives).
- Partitions keep scans small; always **MSCK REPAIR** after writing.
- If the public lake file names shift, use `aws s3 ls` on the public prefixes and adjust copy commands—see `scripts/01_prepare_bronze.sh` for examples and patterns.
- For different regions, ensure your bucket and Glue/Athena/Redshift are in compatible regions.
