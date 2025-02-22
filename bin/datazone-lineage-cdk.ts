#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { DatazoneLineageStack } from '../lib/datazone-lineage-stack';

const app = new cdk.App();
new DatazoneLineageStack(app, 'DatazoneLineageStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION
  },
});