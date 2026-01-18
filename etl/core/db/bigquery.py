from google.cloud import bigquery

from ..env import CoreEnv

bigquery_client = bigquery.Client.from_service_account_json(
  CoreEnv().account_service_instagram_gcp
)