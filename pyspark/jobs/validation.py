import sys

from github_views import GitHubViews
from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
    Usage: validate-damons-datalake
    Validation job to ensure everything is working well
    """
    input_s3_uri = ""
    if len(sys.argv) == 2:
        input_s3_uri = sys.argv[1]
    else:
        raise Exception("Input S3 URI required")

    spark = (
        SparkSession.builder.appName("validate-CurateDamonsDatalake")
        .getOrCreate()
    )

    # Read our data from the appropriate raw table
    df = spark.read.option("recursiveFileLookup", "true").json(input_s3_uri)

    # Step 1 - Get daily views and uniques per repo
    gh = GitHubViews()
    daily_metrics = gh.extract_latest_daily_metrics(df)

    validation_df = daily_metrics.filter(daily_metrics.repo == 'aws-samples/emr-serverless-samples')

    count = validation_df.count()
    min_views = validation_df.agg({"count": "min"}).collect()[0][0]
    max_views = validation_df.agg({"count": "max"}).collect()[0][0]
    assert count>=90, f"expected >=90 records, got: {count}. failing job."
    assert min_views<=max_views, f"expected min_views ({min_views}) <= max_views ({max_views}). failing job."
    assert min_views>0, f"expected min_views>0, got {min_views}. failing job."
    assert max_views>500, f"expected max_views>500, got {max_views}. failing job."

