import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as path from 'path';
import * as cr from 'aws-cdk-lib/custom-resources';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as redshift from 'aws-cdk-lib/aws-redshift';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { CfnCluster } from 'aws-cdk-lib/aws-redshift';


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

    bucket.addToResourcePolicy(new iam.PolicyStatement({
      actions: ['s3:GetBucketAcl'],
      principals: [new iam.ServicePrincipal('redshift.amazonaws.com')],
      resources: [bucket.bucketArn],
    }));    

    // // Function to deploy folders to S3
    // const deployFolderToS3 = (folderName: string, destinationPrefix: string) => {
    //   new s3deploy.BucketDeployment(this, `Deploy${folderName}`, {
    //     sources: [s3deploy.Source.asset(path.join(__dirname, `../data/${folderName}`), { exclude: ['*.pyc'] })],
    //     destinationBucket: bucket,
    //     destinationKeyPrefix: destinationPrefix,
    //   });
    // };

    // // Deploy multiple folders
    // const foldersToDeploy = [
    //   { folderName: 'inventory', destinationPrefix: 'inventory' },
    //   { folderName: 'lib', destinationPrefix: 'lib' },
    //   { folderName: 'scripts', destinationPrefix: 'scripts' },
    // ];

    // foldersToDeploy.forEach((folder) => {
    //   deployFolderToS3(folder.folderName, folder.destinationPrefix);
    // });

    const lambdaLayer = new lambda.LayerVersion(this, 'CfnResponseLayer', {
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/layer')),
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_13, lambda.Runtime.PYTHON_3_12, lambda.Runtime.PYTHON_3_11],
      compatibleArchitectures: [lambda.Architecture.X86_64, lambda.Architecture.ARM_64],
      description: 'Layer for Custom Resource Response',
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
      logRetention: logs.RetentionDays.ONE_MONTH,
      layers: [lambdaLayer],
    });

    copyToS3Lambda.grantInvoke(new iam.ServicePrincipal('lambda.amazonaws.com'));

    // Custom resource to copy files to S3
    const s3CustomResource = new cr.AwsCustomResource(this, 'S3Copy2', {
      onCreate: {
        service: 'Lambda',
        action: 'invoke',
        parameters: {
          FunctionName: copyToS3Lambda.functionName,
          InvocationType: 'RequestResponse',
          Payload: JSON.stringify({
            RequestType: 'Create',
            ResourceProperties: {
              S3BucketName2: bucket.bucketName,
            },
          }),
        },
        physicalResourceId: cr.PhysicalResourceId.of('S3Copy2'),
      },
      onUpdate: {
        service: 'Lambda',
        action: 'invoke',
        parameters: {
          FunctionName: copyToS3Lambda.functionName,
          InvocationType: 'RequestResponse',
          Payload: JSON.stringify({
            RequestType: 'Update',
            ResourceProperties: {
              S3BucketName2: bucket.bucketName,
              // Optionally include a property that changes on every deployment,
              // e.g., a timestamp, to force an update.
              UpdateTime: new Date().toISOString(),
            },
          }),
        },
        physicalResourceId: cr.PhysicalResourceId.of('S3Copy2'),
      },
      // policy: cr.AwsCustomResourcePolicy.fromSdkCalls({
      //   resources: cr.AwsCustomResourcePolicy.ANY_RESOURCE,
      // }),
      policy: cr.AwsCustomResourcePolicy.fromStatements([
        new iam.PolicyStatement({
          actions: ['lambda:InvokeFunction'],
          resources: [copyToS3Lambda.functionArn],
        }),
      ]),

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
      logRetention: logs.RetentionDays.ONE_MONTH,
      layers: [lambdaLayer]
    });

    s3DirLambda.grantInvoke(new iam.ServicePrincipal('lambda.amazonaws.com'));

    // Custom resource to create S3 directories
    const createDirsResource = new cr.AwsCustomResource(this, 'S3CustomResource', {
      onCreate: {
        service: 'Lambda',
        action: 'invoke',
        parameters: {
          FunctionName: s3DirLambda.functionName,
          InvocationType: 'RequestResponse',
          Payload: JSON.stringify({
            RequestType: 'Create',
            ResourceProperties: {
              the_bucket: bucket.bucketName, // Pass the bucket name
              dirs_to_create: ['inventory', 'scripts', 'lib'], // Pass the directories to create
            },
          }),
        },
        physicalResourceId: cr.PhysicalResourceId.of('S3CustomResource'),
      },
      onUpdate: {
        service: 'Lambda',
        action: 'invoke',
        parameters: {
          FunctionName: s3DirLambda.functionName,
          InvocationType: 'RequestResponse',
          Payload: JSON.stringify({
            RequestType: 'Update',
            ResourceProperties: {
              the_bucket: bucket.bucketName, // Pass the bucket name
              dirs_to_create: ['inventory', 'scripts', 'lib'], // Pass the directories to create
            },
          }),
        },
        physicalResourceId: cr.PhysicalResourceId.of('S3CustomResource'),
      },
      // policy: cr.AwsCustomResourcePolicy.fromSdkCalls({
      //   resources: cr.AwsCustomResourcePolicy.ANY_RESOURCE,
      // }),
      policy: cr.AwsCustomResourcePolicy.fromStatements([
        new iam.PolicyStatement({
          actions: ['lambda:InvokeFunction'],
          resources: [s3DirLambda.functionArn],
        }),
      ]),

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
      assumedBy: new iam.CompositePrincipal(
        new iam.ServicePrincipal('glue.amazonaws.com'),
        new iam.ServicePrincipal('datazone.amazonaws.com')
      ),
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

    // Glue Crawler (Fixed: Use bucket.bucketName instead of bucket.bucketArn)
    const crawler = new glue.CfnCrawler(this, 'AWSomeRetailCrawler', {
      name: 'AWSomeRetailCrawler',
      role: glueRole.roleArn,
      databaseName: database.ref,
      targets: {
        s3Targets: [
          { path: `s3://${bucket.bucketName}/inventory/` },
          { path: `s3://${bucket.bucketName}/inventory_insights/` },
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
    s3CustomResource.node.addDependency(createDirsResource);

    /////// Configure Redshift Cluster ///////
    // import custom vpc
    const vpc = ec2.Vpc.fromLookup(this, 'CustomVpc', {
      vpcId: 'vpc-02860dfe7bce18819' // Update this if not available or create new one
    });

    // Security Group for Redshift
    const redshiftSg = new ec2.SecurityGroup(this, 'RedshiftClusterSG', {
      vpc,
      description: 'Allow inbound Redshift (TCP 5439)',
      allowAllOutbound: true,
    });
    redshiftSg.addIngressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(5439),
      'Allow Redshift inbound'
    );

    // Subnet Group for Redshift
    const privateSubnets = vpc.selectSubnets({
      subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
    }).subnetIds;

    // Create a Redshift subnet group
    const subnetGroup = new redshift.CfnClusterSubnetGroup(this, 'RedshiftSubnetGroup', {
      description: 'Subnet group for Redshift cluster',
      subnetIds: privateSubnets,
      // optional: tags
      tags: [
        {
          key: 'Name',
          value: 'RedshiftSubnetGroup',
        },
      ],
    });

    // Create a single-node Redshift cluster
    // WARNING: The "masterPassword" is hardcoded for demonstration. Use Secrets Manager in production.
    const redshiftCluster = new redshift.CfnCluster(this, 'MyRedshiftCluster', {
      clusterType: 'single-node',
      nodeType: 'dc2.large',
      dbName: 'awsome_retail_db',
      masterUsername: 'admin',
      masterUserPassword: 'MySecretPass123',
      clusterIdentifier: 'my-redshift-cluster',

      // Networking
      clusterSubnetGroupName: subnetGroup.ref,
      vpcSecurityGroupIds: [redshiftSg.securityGroupId],
      port: 5439,
      publiclyAccessible: true,  // For demonstration. Typically false in production.

      automatedSnapshotRetentionPeriod: 1,
      allowVersionUpgrade: true,
      // loggingProperties: {
      //   bucketName: bucket.bucketName,
      //   s3KeyPrefix: 'redshift-logs',
      // },
      tags: [
        {
          key: 'Name',
          value: 'DataZoneLineage',
        },
      ]

    });

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

    new cdk.CfnOutput(this, 'RedshiftHostname', {
      value: redshiftCluster.attrEndpointAddress,
      description: 'Hostname for the Redshift cluster endpoint',
    });
    new cdk.CfnOutput(this, 'RedshiftPort', {
      value: redshiftCluster.attrEndpointPort,
      description: 'Port for the Redshift cluster endpoint',
    });

  }
}