import sys

from github_views import GitHubViews
from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
    Usage: curate-damons-datalake <input-s3-uri> <output-s3-uri>
    Creates curated datasets from Damon's Datalake
    """
    input_s3_uri = ""
    output_s3_uri = ""
    if len(sys.argv) == 3:
        input_s3_uri = sys.argv[1]
        output_s3_uri = sys.argv[2]
    else:
        raise Exception("Input and Output S3 URIs required")

    spark = (
        SparkSession.builder.appName("CurateDamonsDatalake")
        .getOrCreate()
    )

    # Read our data from the appropriate raw table
    df = spark.read.option("recursiveFileLookup", "true").json(input_s3_uri)

    # Step 1 - Get daily views and uniques per repo
    gh = GitHubViews()
    daily_metrics = gh.extract_latest_daily_metrics(df)

    # Write out our beautiful data
    daily_metrics.write.mode("overwrite").parquet(output_s3_uri)
