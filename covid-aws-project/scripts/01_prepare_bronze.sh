
#!/usr/bin/env bash
# This script helps you place Bronze CSVs into your bucket.
# It prints discovery commands and includes copy examples.
set -euo pipefail
: "${BUCKET:?Set BUCKET env var}"

ny_dst="s3://$BUCKET/covid/bronze/nytimes/us_states.csv"
ct_dst="s3://$BUCKET/covid/bronze/covid_tracking/states_daily.csv"

echo ">>> Discovering likely NYTimes state CSV in the AWS COVID-19 public lake ..."
aws s3 ls s3://covid19-lake/enigma-nytimes-data-in-usa/ --recursive | grep -Ei 'us[_-]?states.*\.csv$' || true

cat <<'EOF'

# Example copy commands (adjust source after inspecting the listing above):
# aws s3 cp s3://covid19-lake/enigma-nytimes-data-in-usa/<PATH>/us_states.csv  '" + ny_dst + "'
# or:
# aws s3 cp s3://covid19-lake/enigma-nytimes-data-in-usa/<PATH>/us-states.csv  '" + ny_dst + "'

EOF

echo ">>> Discovering likely COVID Tracking 'states daily' CSV in the public lake ..."
aws s3 ls s3://covid19-lake/ --recursive | grep -Ei 'states[_-]?daily.*\.csv$' | head -n 50 || true

cat <<'EOF'

# Example copy commands (adjust source after inspecting the listing above):
# aws s3 cp s3://covid19-lake/<PREFIX>/states_daily.csv   '" + ct_dst + "'
# aws s3 cp s3://covid19-lake/<PREFIX>/states-daily.csv   '" + ct_dst + "'
# aws s3 cp s3://covid19-lake/<PREFIX>/states_daily_4pm_et.csv   '" + ct_dst + "'

# After copying both files, verify in your bucket:
# aws s3 ls s3://$BUCKET/covid/bronze/nytimes/
# aws s3 ls s3://$BUCKET/covid/bronze/covid_tracking/

EOF

echo ">>> Reminder: static lookup already uploaded by 00_upload_code.sh:"
echo "s3://$BUCKET/covid/bronze/static/states_abv.csv"
