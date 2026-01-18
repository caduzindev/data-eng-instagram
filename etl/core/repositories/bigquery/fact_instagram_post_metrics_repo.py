from ...db.bigquery import bigquery_client

from etl.core.entities.instagram import FactInstagramPostMetrics

class FactInstagramPostMetricsRepo:
  table_name = "fact_instagram_post_metrics"

  def save(self, entity: FactInstagramPostMetrics):
    result = bigquery_client.insert_rows_json(
      table=self.table_name,
      json_rows=[entity]
    )

    return result

