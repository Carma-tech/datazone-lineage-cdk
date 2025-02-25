import os
import urllib.request
from urllib.parse import urlparse
import json
import boto3
import cfnresponse

print('Loading function')

s3 = boto3.resource('s3')

def save_to_local(url):
    urlPath = urlparse(url).path
    fileName = os.path.basename(urlPath)
    filePath = '/tmp/' + fileName
    urllib.request.urlretrieve(url, filePath)
    return filePath

def upload_to_s3(filePath, bucket, destination):
    fileName = os.path.basename(filePath)
    s3.Object(bucket, destination + fileName).put(Body=open(filePath, 'rb'))

def copy_to_s3(url, bucket, destination):
    filePath = save_to_local(url)
    upload_to_s3(filePath, bucket, destination)

def lambda_handler(event, context):
    print('Received event: ' + json.dumps(event, indent=2))

    if 'RequestType' in event and event['RequestType'] in ['Create', 'Update']:
        properties = event.get('ResourceProperties', {})
        urls = [
            ("https://aws-blogs-artifacts-public.s3.amazonaws.com/BDB-4447/data/inventory.csv", "inventory/"),
            ("https://aws-blogs-artifacts-public.s3.amazonaws.com/BDB-4447/scripts/Inventory_Insights.py", "scripts/"),
            ("https://aws-blogs-artifacts-public.s3.amazonaws.com/BDB-4447/lib/openlineage-spark_2.12-1.9.1.jar", "lib/")
        ]
        bucket = properties.get('S3BucketName2')
        
        try:
            for url, destination in urls:
                copy_to_s3(url, bucket, destination)
        except Exception as e:
            print("Error:", e)
            if "ResponseURL" in event:
                cfnresponse.send(event, context, cfnresponse.FAILED, {'Response': 'Failure'})
            return
    
    # Only send a CloudFormation response if ResponseURL exists
    if "ResponseURL" in event:
        cfnresponse.send(event, context, cfnresponse.SUCCESS, {'Response': 'Success'})
    else:
        print("Lambda invoked without CloudFormation ResponseURL, skipping cfnresponse.send()")
