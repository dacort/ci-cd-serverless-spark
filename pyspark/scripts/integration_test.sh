#!/usr/bin/env bash

# This script kicks off an EMR Serverless job and waits for it to complete.
# If the job does not run successfully, the script errors out.
APPLICATION_ID=$1
JOB_ROLE_ARN=$2
S3_BUCKET=$3
JOB_VERSION=$4
ENTRY_POINT=$5
SPARK_JOB_PARAMS=(${@:6})

# Convert the passed Spark job params into a JSON array
# WARNING: Assumes there are job params
printf -v SPARK_ARGS '"%s",' "${SPARK_JOB_PARAMS[@]}"

# Start the job
JOB_RUN_ID=$(aws emr-serverless start-job-run \
    --name ghuni-${ENTRY_POINT} \
    --application-id $APPLICATION_ID \
    --execution-role-arn $JOB_ROLE_ARN \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'${S3_BUCKET}'/github/pyspark/jobs/'${JOB_VERSION}'/'${ENTRY_POINT}'",
            "entryPointArguments": ['${SPARK_ARGS%,}'],
            "sparkSubmitParameters": "--py-files s3://'${S3_BUCKET}'/github/pyspark/jobs/'${JOB_VERSION}'/job_files.zip"
        }
    }' --query 'jobRunId' --output text)

echo "Job submitted: ${APPLICATION_ID}/${JOB_RUN_ID}"

# Wait for it to complete
JOB_STATUS="running"
while [ "$JOB_STATUS" != "SUCCESS" -a "$JOB_STATUS" != "FAILED" ]; do
    sleep 30
    JOB_STATUS=$(aws emr-serverless get-job-run --application-id $APPLICATION_ID --job-run-id $JOB_RUN_ID --query 'jobRun.state' --output text)
    echo "Job ($JOB_RUN_ID) status is: ${JOB_STATUS}"
done

if [ "$JOB_STATUS" = "FAILED" ]; then
    ERR_MESSAGE=$(aws emr-serverless get-job-run --application-id $APPLICATION_ID --job-run-id $JOB_RUN_ID --query 'jobRun.stateDetails' --output text)
    echo "Job failed: ${ERR_MESSAGE}"
    exit 1;
fi
