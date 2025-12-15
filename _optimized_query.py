import os
import pandas as pd

from google.cloud import bigquery
from google.cloud import bigquery_storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "keys/bq-github-pj-f2d6609f5288.json"

# Load org IDs
org_ids = pd.read_csv("org_id_only.csv", dtype={"org_id": "Int64"})["org_id"].tolist()

# Build query
query = """
-- Monthly GitHub Archive org metrics (optimized to only read payload when needed)

WITH
  -- Create a virtual table with months to ensure all months are represented
  months AS (
    SELECT
      FORMAT_DATE('%Y-%m', month) AS month
    FROM UNNEST(
      GENERATE_DATE_ARRAY(
        DATE '2015-01-01',
        DATE '2025-11-01',
        INTERVAL 1 MONTH
      )
    ) AS month
  ),

  -- 1) All activity WITHOUT payload (cheap columns only)
  activity AS (
    SELECT
      org.id AS org_id,
      actor.id AS actor_id,
      repo.id AS repo_id,
      type,
      FORMAT_DATE('%Y-%m', DATE(created_at)) AS month
    FROM `githubarchive.month.*`
    WHERE _TABLE_SUFFIX BETWEEN '201501' AND '202511'
      AND org.id IN UNNEST(@org_ids)
  ),

  activity_agg AS (
    SELECT
      org_id,
      month,
      COUNTIF(type = 'PushEvent') AS push_requests,
      COUNT(DISTINCT actor_id) AS active_developers,
      COUNTIF(type = 'ForkEvent') AS forks,
      SAFE_DIVIDE(
        COUNTIF(type = 'ForkEvent'),
        COUNT(DISTINCT IF(type = 'ForkEvent', repo_id, NULL)) --NULL for all other event types it will be NULL and wont be counted when DISTINCT is applied
      ) AS forks_per_repo,
      COUNTIF(type = 'WatchEvent') AS stars,
      COUNT(DISTINCT repo_id) AS active_repos,
      COUNTIF(type = 'PublicEvent') AS changed_to_public_repo
    FROM activity
    GROUP BY org_id, month
  ),

  -- 2) Commits: payload only for PushEvent
  commits AS (
    SELECT
      org.id AS org_id,
      FORMAT_DATE('%Y-%m', DATE(created_at)) AS month,
      SUM(SAFE_CAST(JSON_VALUE(payload, '$.distinct_size') AS INT64)) AS commit_count
    FROM `githubarchive.month.*`
    WHERE _TABLE_SUFFIX BETWEEN '201501' AND '202511'
      AND org.id IN UNNEST(@org_ids)
      AND type = 'PushEvent'
    GROUP BY org_id, month
  ),

  -- 3) Public repositories created: payload only for CreateEvent
  public_repos AS (
    SELECT
      org.id AS org_id,
      FORMAT_DATE('%Y-%m', DATE(created_at)) AS month,
      COUNT(*) AS public_repos
    FROM `githubarchive.month.*`
    WHERE _TABLE_SUFFIX BETWEEN '201501' AND '202511'
      AND org.id IN UNNEST(@org_ids)
      AND type = 'CreateEvent'
      AND JSON_VALUE(payload, '$.ref_type') = 'repository'
    GROUP BY org_id, month
  ),

  -- 4) PR metrics: payload only for PullRequestEvent
  pr_metrics AS (
    SELECT
      org.id AS org_id,
      FORMAT_DATE('%Y-%m', DATE(created_at)) AS month,
      COUNTIF(JSON_VALUE(payload, '$.action') = 'opened') AS pull_requests_created,
      COUNTIF(JSON_VALUE(payload, '$.pull_request.merged') = 'true') AS pull_requests_merged
    FROM `githubarchive.month.*`
    WHERE _TABLE_SUFFIX BETWEEN '201501' AND '202511'
      AND org.id IN UNNEST(@org_ids)
      AND type = 'PullRequestEvent'
    GROUP BY org_id, month
  )

SELECT
  org_id,
  m.month,
  IFNULL(a.push_requests, 0) AS push_requests_per_month,
  IFNULL(c.commit_count, 0) AS commits_per_month,
  IFNULL(a.active_developers, 0) AS active_developers_per_month,
  IFNULL(a.forks, 0) AS forks_per_month,
  IFNULL(a.forks_per_repo, 0) AS forks_per_repo_per_month,
  IFNULL(a.stars, 0) AS total_stars_per_month,
  IFNULL(a.active_repos, 0) AS active_repos_per_month,
  IFNULL(a.changed_to_public_repo, 0) AS changed_to_public_repo_per_month,
  IFNULL(pr.public_repos, 0) AS public_repos_per_month,
  IFNULL(p.pull_requests_created, 0) AS pull_requests_created_per_month,
  IFNULL(p.pull_requests_merged, 0) AS pull_requests_merged_per_month
FROM UNNEST(@org_ids) AS org_id
CROSS JOIN months AS m
LEFT JOIN activity_agg a
  ON a.org_id = org_id AND a.month = m.month
LEFT JOIN commits c
  ON c.org_id = org_id AND c.month = m.month
LEFT JOIN public_repos pr
  ON pr.org_id = org_id AND pr.month = m.month
LEFT JOIN pr_metrics p
  ON p.org_id = org_id AND p.month = m.month
ORDER BY org_id, m.month;
"""

# Configure parameters
job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ArrayQueryParameter("org_ids", "INT64", org_ids)
    ]
)

# Run
client = bigquery.Client()

bqstorage_client = bigquery_storage.BigQueryReadClient()

df = client.query(query, job_config=job_config).to_dataframe(
    bqstorage_client=bqstorage_client)

df.to_parquet("gitarchive_results_jan2015_nov2025.parquet")
print(df)