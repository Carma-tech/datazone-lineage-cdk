#!/usr/bin/env bash
#
# Usage:
#   ./automate_redshift_lineage.sh <CLUSTER_ID> <DATAZONE_DOMAIN_ID>
#
# Example:
#   ./automate_redshift_lineage.sh my-redshift-cluster dzd_YourDomain
#
# Description:
#   1. Finds the Redshift endpoint and port via `describe-clusters`.
#   2. Creates the `market` schema, tables, and sample data using the Redshift Data API.
#   3. Runs extract_redshift_lineage.py to post lineage to Amazon DataZone.
#
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "Usage: $0 <CLUSTER_ID> <DATAZONE_DOMAIN_ID>"
  exit 1
fi

CLUSTER_ID="$1"
DATAZONE_DOMAIN_ID="$2"

########################################
# CONFIGURATION (Adjust as needed)
########################################

REGION="us-east-1"
DB_USER="admin"             # Must have permission to create schemas/tables
DATABASE_NAME="awsome_retail_db"    # Default DB name used by your cluster
START_TIME="2025-02-26T00:00:00Z"
REDSHIFT_LINEAGE_SCRIPT="datazone-examples/extract_redshift_lineage.py"  # Must exist locally

########################################
# 1) LOOK UP THE REDSHIFT ENDPOINT & PORT
########################################
# We query the cluster for its Endpoint info.

ENDPOINT=$(aws redshift describe-clusters \
  --region "$REGION" \
  --cluster-identifier "$CLUSTER_ID" \
  --query 'Clusters[0].Endpoint.Address' \
  --output text)

PORT=$(aws redshift describe-clusters \
  --region "$REGION" \
  --cluster-identifier "$CLUSTER_ID" \
  --query 'Clusters[0].Endpoint.Port' \
  --output text)

if [ "$ENDPOINT" = "None" ] || [ "$PORT" = "None" ]; then
  echo "Failed to find an endpoint or port for cluster '$CLUSTER_ID' in region '$REGION'."
  exit 1
fi

echo "Found Redshift endpoint: $ENDPOINT"
echo "Found Redshift port: $PORT"

########################################
# 2) CREATE SCHEMA/TABLES WITH THE REDSHIFT DATA API
########################################
# We'll do the same multi-line SQL from the blog.

SQL="
Create SCHEMA market;

create table market.retail_sales (
  id BIGINT primary key,
  name character varying not null
);

create table market.online_sales (
  id BIGINT primary key,
  name character varying not null
);

/* Important to insert some data in the table */
INSERT INTO market.retail_sales
VALUES (123, 'item1');

INSERT INTO market.online_sales
VALUES (234, 'item2');

create table market.sales AS
Select id, name from market.retail_sales
Union ALL
Select id, name from market.online_sales;
"

echo "Executing Redshift statements to set up 'market' schema and tables..."

STATEMENT_ID=$(aws redshift-data execute-statement \
  --region "$REGION" \
  --cluster-identifier "$CLUSTER_ID" \
  --database "$DATABASE_NAME" \
  --db-user "$DB_USER" \
  --sql "$SQL" \
  --query 'Id' \
  --output text)

echo "Statement ID: $STATEMENT_ID"
echo "Waiting for statement to finish..."

while true; do
  STATUS=$(aws redshift-data describe-statement \
    --id "$STATEMENT_ID" \
    --region "$REGION" \
    --query 'Status' \
    --output text)

  if [ "$STATUS" == "FINISHED" ]; then
    echo "Redshift setup statements completed successfully."
    break
  elif [ "$STATUS" == "FAILED" ] || [ "$STATUS" == "ABORTED" ]; then
    echo "Redshift statement ended with status: $STATUS"
    ERROR_MSG=$(aws redshift-data describe-statement \
      --id "$STATEMENT_ID" \
      --region "$REGION" \
      --query 'Error' \
      --output text || true)
    echo "Error: $ERROR_MSG"
    exit 1
  else
    echo "Current status: $STATUS ... sleeping 5s"
    sleep 5
  fi
done

########################################
# 3) RUN THE REDSHIFT LINEAGE SCRIPT
########################################
if [ ! -f "$REDSHIFT_LINEAGE_SCRIPT" ]; then
  echo "Error: $REDSHIFT_LINEAGE_SCRIPT not found!"
  exit 1
fi

echo "Running Redshift lineage script: $REDSHIFT_LINEAGE_SCRIPT"
python "$REDSHIFT_LINEAGE_SCRIPT" \
  -r "$REGION" \
  -i "$DATAZONE_DOMAIN_ID" \
  -n "$ENDPOINT" \
  -t "$PORT" \
  -d "$DATABASE_NAME" \
  -s "$START_TIME"

echo "Done! Your Redshift lineage should now be posted to DataZone domain: $DATAZONE_DOMAIN_ID"

aws datazone start-data-source-run \
  --domain-identifier $DATAZONE_DOMAIN_ID \
  --identifier "Sales_DW_Enviroment-default-datasource"
