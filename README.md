# Serverless Spark CI/CD on AWS with GitHub Actions

This is the code used for my [GitHub Universe presentation](https://githubuniverse.com/events/detail/virtual-schedule/0a4737d4-4e63-4b50-8) on using GitHub Actions with EMR Serverless.

## Other resources

- [EMR Serverless User Guide](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html)
- [EMR Serverless Samples](https://github.com/aws-samples/emr-serverless-samples/)

## Pre-requisites

- An AWS Account with Admin privileges
- GitHub OIDC Provider in AWS
- S3 Bucket(s)
- EMR Serverless Spark application(s)
- IAM Roles for GitHub and EMR Serverless

You can create all of these, including some sample data, using the included [CloudFormation template](template.cfn.yaml).

> **Warning** 💰 The CloudFormation template creates EMR Serverless applications that you will be charged for when integration tests **AND** the scheduled workflow runs.

> **Note** The IAM roles created in the template are _very tightly scoped_ to the relevant S3 Buckets and EMR Serverless applications created by the stack.

## Setup

To follow along, just fork this repository into your own account, clone it locally and do the following:

1. Create the CloudFormation Stack

```shell
aws cloudformation create-stack \
    --stack-name gh-severless-spark-demo \
    --template-body file://./template.cfn.yaml \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameters ParameterKey=GitHubRepo,ParameterValue=USERNAME/REPO ParameterKey=CreateOIDCProvider,ParameterValue=true
```

- `GitHubRepo` is the `user/repo` format of your GitHub repository that you want your OIDC role to be able to access.
- `CreateOIDCProvider` allows you to disable creating the OIDC endpoint for GitHub in your AWS account if it already exists.

2. Create an "Actions" Secret in your repo

Go to your repository settings, find `Secrets` on the left-hand side, then `Actions`. Click "New repository secret" and add a secret named `AWS_ACCOUNT_ID` with your 12 digit AWS Account ID.

> **Note** This is not sensitive info, just makes it easier to re-use the Actions.

3. Update the Application IDs

- In `integration-test.yaml`, replace `TEST_APPLICATION_ID` with the `TestApplicationId` output from the CloudFormation stack
- In `run-job.yaml`, replace `PROD_APPLICATION_ID` with the `ProductionApplicationId` output from the CloudFormation stack

The rest of the environment variables in your workflows should stay the same unless you deployed in a region other than `us-east-1`.

With that done, you should be able to experiment with pushing new commits to the repo, opening pull requests, and running the "Fetch Data" workflow.

You can view the status of your job runs in the [EMR Serverless console](https://console.aws.amazon.com/emr/home#/serverless).

## Overview

The demo goes into 4 specific use cases, each defined as part of a different GitHub Action. These are intended to be easily reusable

### Creating a simple pytest unit test

The [`unit-tests.yaml`](.github/workflows/unit-tests.yaml) file defines a very simple GitHub Action that runs on any `push` event. It runs the tests in the [pyspark/tests/test_basic.py](./pyspark/tests/test_basic.py).

### Running integration tests on EMR Serverless

[`integration-test.yaml`](.github/workflows/integration-test.yaml) runs on any Pull Request and both 1/ copies the local pyspark code to S3 and 2/ runs an EMR Serverless job and waits until it's complete.

### Deploying EMR Serverless PySpark job to S3

When a semantic-versioned tag is added to the repository, [`deploy.yaml`](.github/workflows/deploy.yaml) zips up files in the `jobs` folder, and copies the zip and `main.py` files to S3 in a location with the tag as part of the prefix.

### Running ETL jobs manualy or on a schedule

[`run-job.yaml`](.github/workflows/run-job.yaml) runs the `main.py` script on a schedule with the version defined in the `JOB_VERSION` variable. The `workflow_dispatch` section also lets you run the job manually, which by default uses the "latest" semantic tag on the repository.
