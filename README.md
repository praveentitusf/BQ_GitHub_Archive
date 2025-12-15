# GitHub Archive BigQuery Export

This script queries GitHub Archive data from BigQuery and exports monthly
organization-level metrics to a Parquet file.

## Requirements
- Python 3.13.9
- Google Cloud project with BigQuery enabled
- Service account with BigQuery access

## Install dependencies
```bash
pip install pandas google-cloud-bigquery google-cloud-bigquery-storage pyarrow
```
Note: Store the service key for bigquery from google in the folder named Keys in the root of the project. 

```
- To run query use the script _optimized_query.py
```
