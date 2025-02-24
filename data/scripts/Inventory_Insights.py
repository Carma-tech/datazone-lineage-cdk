import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Fetch AWS account number
sts_client = boto3.client('sts')
account_id = sts_client.get_caller_identity()['Account']

# Load data from the Glue Catalog as a DataFrame
df = spark.sql("SELECT * FROM awsome_retail_db.inventory")

# Log initial DataFrame count
print(f"Initial DataFrame count: {df.count()}")

# Show schema to verify column names and data types
df.printSchema()

# Show sample data
df.show(5)

# Group by product_id and sum the inventory_quantity
grouped_df = df.groupBy("product_id").sum("inventory_quantity").withColumnRenamed("sum(inventory_quantity)", "total_inventory")
print(f"Grouped DataFrame count: {grouped_df.count()}")

# Show sample data from the grouped DataFrame
grouped_df.show()

# Construct the S3 path including the account number
s3_path = f"s3://dzlineageblog-{account_id}/inventory_insights/"

# Write the data out in Parquet format to S3, overwriting existing data
grouped_df.write.mode("overwrite").format("parquet").save(s3_path)

job.commit()