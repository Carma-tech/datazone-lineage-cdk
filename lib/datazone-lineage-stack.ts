import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as path from 'path';
import { CustomResource } from 'aws-cdk-lib';

export class DatazoneLineageStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3 Bucket
    const bucket = new s3.Bucket(this, 'S3Bucket', {
      accessControl: s3.BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
      bucketName: `dzlineageblog-${cdk.Aws.ACCOUNT_ID}`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // Lambda Execution Role for Copying Files to S3
    const lambdaExecutionRole = new iam.Role(this, 'LambdaExecutionRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      path: '/',
      inlinePolicies: {
        root: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: [
                'logs:CreateLogGroup',
                'logs:CreateLogStream',
                'logs:PutLogEvents',
                'logs:DescribeLogStreams',
              ],
              resources: [`arn:aws:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:*`],
            }),
            new iam.PolicyStatement({
              actions: ['s3:PutObject', 's3:GetObject'],
              resources: [bucket.arnForObjects('*')],
            }),
          ],
        }),
      },
    });

    // Lambda Function to Copy Files to S3
    const copyToS3Lambda = new lambda.Function(this, 'CopyCustomersToS3', {
      handler: 'copy_to_s3.lambda_handler',
      role: lambdaExecutionRole,
      runtime: lambda.Runtime.PYTHON_3_12,
      timeout: cdk.Duration.seconds(60),
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda')),
    });

    // Custom Resource to Copy Files to S3
    const s3CopyResource = new CustomResource(this, 'S3Copy2', {
      serviceToken: copyToS3Lambda.functionArn,
      properties: {
        S3BucketName2: bucket.bucketName,
      },
    });

    // Lambda Execution Role for S3 Directory Creation
    const awsLambdaExecutionRole = new iam.Role(this, 'AWSLambdaExecutionRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      roleName: `${cdk.Aws.STACK_NAME}-${cdk.Aws.REGION}-AWSLambdaExecutionRole`,
      path: '/',
      inlinePolicies: {
        'AWSLambda-CW': new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
              resources: [`arn:aws:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:/aws/lambda/${copyToS3Lambda.functionName}:*`],
            }),
          ],
        }),
        'AWSLambda-S3': new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: ['s3:PutObject', 's3:DeleteObject', 's3:ListObject'],
              resources: [bucket.bucketArn, bucket.arnForObjects('*')],
            }),
          ],
        }),
      },
    });

    // Lambda Function to Create S3 Directories
    const s3DirLambda = new lambda.Function(this, 'AWSLambdaFunction', {
      description: 'Work with S3 Buckets!',
      functionName: `${cdk.Aws.STACK_NAME}-${cdk.Aws.REGION}-lambda`,
      handler: 'create_s3_dirs.handler',
      role: awsLambdaExecutionRole,
      timeout: cdk.Duration.seconds(360),
      runtime: lambda.Runtime.PYTHON_3_12,
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda')),
    });

    // Custom Resource to Create S3 Directories
    const s3CustomResource = new CustomResource(this, 'S3CustomResource', {
      serviceToken: s3DirLambda.functionArn,
      properties: {
        the_bucket: bucket.bucketName,
        dirs_to_create: ['scripts', 'lib'],
      },
    });

    // Glue Database
    const database = new glue.CfnDatabase(this, 'CFNDatabaseWS', {
      catalogId: cdk.Aws.ACCOUNT_ID,
      databaseInput: {
        name: 'awsome_retail_db',
        description: 'Database for AWSome Retail',
      },
    });

    // Glue Role
    const glueRole = new iam.Role(this, 'CFNGlueRole', {
      roleName: 'DZBlogRole',
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      path: '/',
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
      ],
      inlinePolicies: {
        AccessPolicies: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: ['*'],
              resources: [bucket.arnForObjects('*')],
            }),
          ],
        }),
      },
    });

    // Glue Crawler
    const crawler = new glue.CfnCrawler(this, 'AWSomeRetailCrawler', {
      name: 'AWSomeRetailCrawler',
      role: glueRole.roleArn,
      databaseName: database.ref,
      targets: {
        s3Targets: [
          { path: `${bucket.bucketArn}/inventory/` },
          { path: `${bucket.bucketArn}/inventory_insights/` },
        ],
      },
      schemaChangePolicy: {
        updateBehavior: 'UPDATE_IN_DATABASE',
        deleteBehavior: 'DEPRECATE_IN_DATABASE',
      },
    });

    // Glue Job
    const glueJob = new glue.CfnJob(this, 'InventoryInsightsJob', {
      name: 'inventory_insights',
      role: glueRole.roleArn,
      command: {
        name: 'glueetl',
        scriptLocation: bucket.s3UrlForObject('scripts/Inventory_Insights.py'),
        pythonVersion: '3',
      },
      defaultArguments: {
        '--conf': 'spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener \
        --conf spark.openlineage.transport.type=console \
        --conf spark.openlineage.facets.custom_environment_variables=[AWS_DEFAULT_REGION;GLUE_VERSION;GLUE_COMMAND_CRITERIA;GLUE_PYTHON_VERSION;]',
        '--user-jars-first': 'true',
        '--enable-glue-datacatalog': 'true',
        '--job-bookmark-option': 'job-bookmark-disable',
        '--extra-jars': bucket.s3UrlForObject('lib/openlineage-spark_2.12-1.9.1.jar'),
      },
      glueVersion: '4.0',
      maxCapacity: 2.0,
      timeout: 60,
    });

    // Ensure S3 directories are created before copying files
    s3CopyResource.node.addDependency(s3CustomResource);

    // Outputs
    new cdk.CfnOutput(this, 'BucketName', {
      value: bucket.bucketName,
      description: 'S3 Bucket',
    });
    new cdk.CfnOutput(this, 'GlueDatabase', {
      value: database.ref,
      description: 'Glue Database',
    });
    new cdk.CfnOutput(this, 'GlueCrawlerName', {
      value: crawler.name!,
      description: 'AWS Glue Crawler Name',
    });
    new cdk.CfnOutput(this, 'GlueJobName', {
      value: glueJob.name!,
      description: 'AWS Glue Job Name',
    });
  }
}