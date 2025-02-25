# Amazon DataZone Lineage Visualization with AWS CDK

This repository demonstrates how to deploy **Amazon DataZone** for data lineage visualization using **AWS CDK (TypeScript)**. The steps in this guide automate the process outlined in the [AWS Big Data Blog post](https://aws.amazon.com/blogs/big-data/amazon-datazone-introduces-openlineage-compatible-data-lineage-visualization-in-preview/), allowing users to implement OpenLineage-compatible lineage tracking using AWS CDK.

---

## Prerequisites

Ensure you have the following installed and configured before proceeding:

- **AWS Account**: An active AWS account with necessary permissions.
- **AWS CLI v2**: Installed and configured.
- **Node.js**: Version 18.17.0 or higher.
- **npm**: Version 10.2.0 or higher.
- **AWS CDK**: Installed globally.
  ```bash
  npm install -g aws-cdk
  ```
- **TypeScript**: Installed globally.
  ```bash
  npm install -g typescript
  ```
- **AWS IAM Identity Center**: Enabled in your AWS account.
- **AWS Glue Table**: An existing AWS Glue table to register as a sample data source in Amazon DataZone.

---

## Setup Instructions

### 1. Clone and Install Project Dependencies

Clone this repository and install the required dependencies.

```bash
git clone https://github.com/Carma-tech/datazone-lineage-cdk.git
cd datazone-lineage-cdk
npm install
```

### 2. Deploy the Stack

Run the following commands to deploy the AWS resources:

```bash
cdk bootstrap
cdk deploy
```

This will provision the necessary resources, including:
- S3 bucket for data storage.
- AWS Glue database and tables.
- AWS Glue crawlers.
- AWS Glue ETL job for processing data.
- Lambda functions for lineage tracking.

---

## Capturing Lineage from AWS Glue Tables

After deploying the CDK stack, follow these steps to capture lineage metadata.

### **1. Run the AWS Glue Crawler**
1. Navigate to the **AWS Glue Console**.
2. Choose **Crawlers** from the left navigation pane.
3. Select `AWSomeRetailCrawler` (created by CDK) and choose **Run**.
4. Wait until the crawler **Succeeds**.

### **2. Extract Lineage Metadata**
Run the following commands in **CloudShell**:

```bash
sudo yum -y install python3
python3 -m venv env
. env/bin/activate
pip install boto3
```

Download and execute the lineage extraction script:
```bash
wget https://example.com/extract_glue_crawler_lineage.py
python extract_glue_crawler_lineage.py -d awsome_retail_db -t inventory -r us-east-1 -i dzd_YourDomain
```

Confirm the settings when prompted and verify a success notification.

---

## Capturing Lineage from AWS Glue ETL Jobs

1. Go to the **AWS Glue Console**.
2. Select the `Inventory_Insights` job and **Run**.

### **Extract AWS Glue Job Lineage**
```bash
wget https://example.com/extract_glue_spark_lineage.py
python extract_glue_spark_lineage.py --region us-east-1 --domain-identifier 'dzd_YourDomain'
```

After running the script, AWS Glue job lineage metadata will be available.

---

## Capturing Lineage from Amazon Redshift

Amazon Cloud9 is used to capture lineage from Amazon Redshift.

1. **Create required tables**:
```sql
CREATE SCHEMA market;
CREATE TABLE market.retail_sales (id BIGINT PRIMARY KEY, name VARCHAR NOT NULL);
CREATE TABLE market.online_sales (id BIGINT PRIMARY KEY, name VARCHAR NOT NULL);
INSERT INTO market.retail_sales VALUES (123, 'item1');
INSERT INTO market.online_sales VALUES (234, 'item2');
CREATE TABLE market.sales AS
SELECT id, name FROM market.retail_sales
UNION ALL
SELECT id, name FROM market.online_sales;
```

2. **Run lineage extraction**:
```bash
wget https://example.com/extract_redshift_lineage.py
python extract_redshift_lineage.py -r us-east-1 -i dzd_YourDomain -n your-redshift-cluster-endpoint -t your-rs-port -d your-database -s the-starting-date
```

---

## Capturing Lineage from Amazon MWAA (Managed Workflows for Apache Airflow)

### **1. Configure OpenLineage Plugin in MWAA**
Add the following to **requirements.txt** in your MWAA environment:
```
openlineage-airflow==1.4.1
```

Enable **Airflow task logs** at **INFO** level in the MWAA configuration.

### **2. Extract MWAA Lineage Metadata**
```bash
wget https://example.com/extract_airflow_lineage.py
python extract_airflow_lineage.py --region us-east-1 --domain-identifier your_domain_identifier --airflow-environment-name your_airflow_environment_name
```

---

## Viewing Data Lineage in Amazon DataZone

1. Navigate to the **Amazon DataZone Console**.
2. Open your **Sales** project.
3. In the **Data Inventory** tab, select the `Inventory` table.
4. Click on the **Lineage** tab to view the generated lineage diagram.

---

## Cleanup

To delete the stack and remove all resources, run:

```bash
cdk destroy
```

---

## Key AWS Resources Created

- **S3 Bucket**: Stores inventory data, scripts, and lineage-related artifacts.
- **AWS Glue Database**: Manages metadata for the inventory dataset.
- **AWS Glue Crawler**: Scans S3 data and populates the Glue Catalog.
- **AWS Glue ETL Job**: Processes inventory data and generates insights.
- **Lambda Functions**: Automate lineage metadata extraction and ingestion.

---

## Troubleshooting

- **Lambda Function Errors**:
  - Check **CloudWatch Logs** for any errors.
  - Ensure that `cfnresponse` is included in the Lambda package or attached as a layer.

- **Stack Deletion Issues**:
  - Manually delete S3 bucket contents before running `cdk destroy`.
  - Remove any manually created IAM roles or permissions.

---

## References

- [AWS Big Data Blog Post](https://aws.amazon.com/blogs/big-data/amazon-datazone-introduces-openlineage-compatible-data-lineage-visualization-in-preview/)
- [AWS CDK Documentation](https://docs.aws.amazon.com/cdk/v2/guide/home.html)

---

