
# Running locally with Spark (optional notes)

If you insist on local testing:
- Install Apache Spark 3.x + Java 8/11.
- Configure `hadoop-aws` and proper AWS credentials so Spark can read/write S3 (fs.s3a.*).
- Then run:
  ```bash
  export BUCKET=YOUR_BUCKET
  spark-submit code/silver_job.py --BUCKET $BUCKET
  spark-submit code/gold_job.py   --BUCKET $BUCKET
  ```

However, **AWS Glue** is strongly recommended for simplicity.
