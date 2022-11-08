from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode


class GitHubViews:
    def extract_latest_daily_metrics(self, df: DataFrame) -> DataFrame:
        # First explode the `stats.views` array into multiple rows
        df2 = df.select(
            df.repo, df.timestamp, df.stats.count, df.stats.uniques, explode(df.stats.views)
        )

        # Then we pull out the columns and rename them
        dfDaily = df2.select(df2.repo, df2.col.timestamp, df2.col.count, df2.col.uniques).toDF(
            *["repo", "timestamp", "count", "uniques"]
        )

        # Finally, we select the "max daily values" for each day and convert our timestamp to a date column
        dfMax = (
            dfDaily.withColumn("date", col("timestamp").cast("date"))
            .groupBy("repo", "date")
            .max("count", "uniques")
            .withColumnRenamed('max(count)', 'count')
            .withColumnRenamed('max(uniques)', 'uniques')
        )

        return dfMax
