#!/usr/bin/env bash
#
# postdeploy.sh - Automate lineage extraction without using the AWS Console
#
# Usage:
#   ./scripts/postdeploy.sh <DATAZONE_DOMAIN_ID>
#
# Example:
#   ./scripts/postdeploy.sh dzd_6wbtsa8ioowucn
#

set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <DATAZONE_DOMAIN_ID>"
  exit 1
fi

DATAZONE_DOMAIN_ID=$1
REGION="us-east-1"
CRAWLER_NAME="AWSomeRetailCrawler"
GLUE_JOB_NAME="inventory_insights"
DATABASE_NAME="awsome_retail_db"
TABLE_NAME="inventory"

### 1. Start the AWS Glue Crawler ###
echo "Starting AWS Glue crawler: ${CRAWLER_NAME}"
aws glue start-crawler --name "${CRAWLER_NAME}" --region "${REGION}"

echo "Waiting for crawler to complete..."
while true; do
  STATUS=$(aws glue get-crawler --name "${CRAWLER_NAME}" --query 'Crawler.State' --output text --region "${REGION}")
  if [ "$STATUS" == "READY" ]; then
    echo "Crawler complete."
    break
  fi
  sleep 10
done

### 2. Run the AWS Glue Job ###
# echo "Starting AWS Glue Job: ${GLUE_JOB_NAME}"
# JOB_RUN_ID=$(aws glue start-job-run --job-name "${GLUE_JOB_NAME}" --region "${REGION}" --query 'JobRunId' --output text)

# echo "Glue Job Run ID: $JOB_RUN_ID"
# echo "Waiting for the job to complete..."
# while true; do
#   JOB_STATUS=$(aws glue get-job-run --job-name "${GLUE_JOB_NAME}" --run-id "$JOB_RUN_ID" --region "${REGION}" --query 'JobRun.JobRunState' --output text)
#   if [ "$JOB_STATUS" == "SUCCEEDED" ]; then
#     echo "Glue job succeeded."
#     break
#   elif [ "$JOB_STATUS" == "FAILED" ] || [ "$JOB_STATUS" == "STOPPED" ] || [ "$JOB_STATUS" == "TIMEOUT" ]; then
#     echo "Glue job ended with status: $JOB_STATUS"
#     exit 1
#   fi
#   sleep 10
# done


### 3. Harvest lineage: Glue Crawler ###
echo "Extracting lineage for AWS Glue crawler table: ${TABLE_NAME}"
python datazone-examples/extract_glue_crawler_lineage.py \
  -d "${DATABASE_NAME}" \
  -t "${TABLE_NAME}" \
  -r "${REGION}" \
  -i "${DATAZONE_DOMAIN_ID}"

### 4. Harvest lineage: Glue Spark Job ###
echo "Extracting lineage for AWS Glue Spark job logs"
# python datazone-examples/extract_glue_spark_lineage.py \
#   --region "${REGION}" \
#   --domain-identifier "${DATAZONE_DOMAIN_ID}"
#   --max-seconds 300
echo "Running Spark lineage script (15-second limit) in background..."
python datazone-examples/extract_glue_spark_lineage.py \
  --region "${REGION}" \
  --domain-identifier "${DATAZONE_DOMAIN_ID}" \
  --max-seconds 15 &

LINEAGE_PID=$!

echo "Sleeping 15 seconds..."
sleep 15

echo "Now running Glue job again automatically..."
aws glue start-job-run \
  --job-name "${GLUE_JOB_NAME}" \
  --region "${REGION}"

echo "Waiting for lineage script to finish..."
wait "${LINEAGE_PID}"

echo
echo "All done! Data lineage should now be visible in Amazon DataZone."
