from jobs.github_views import GitHubViews


def test_s3_uri():
    assert 1 == 1


def test_extract_latest_daily_value(mock_views_df):
    ghv = GitHubViews()
    metrics = ghv.extract_latest_daily_metrics(mock_views_df)

    # There should be 115 records from 2022-05-31 to 2022-09-22 (our test dataset)
    assert metrics.count() == 115

    assert metrics.agg({"date": "min"}).first()[0].strftime("%Y-%m-%d") == "2022-05-31"
    assert metrics.agg({"date": "max"}).first()[0].strftime("%Y-%m-%d") == "2022-09-22"
