
#!/usr/bin/env bash
set -euo pipefail
: "${BUCKET:?Set BUCKET env var}"

echo "Syncing code and SQL to s3://$BUCKET ..."
aws s3 sync ./code "s3://$BUCKET/code" --delete
aws s3 sync ./sql  "s3://$BUCKET/sql"  --delete

echo "Uploading static lookup to Bronze ..."
aws s3 cp ./data/static/states_abv.csv "s3://$BUCKET/covid/bronze/static/states_abv.csv"

echo "Done."
