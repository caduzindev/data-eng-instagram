from ...db.bigquery import bigquery_client

from etl.core.entities.instagram import FactInstagramCommentMetrics

class FactInstagramCommentMetricsRepo:
  table_name = "fact_instagram_comment_metrics"

  def save(self, entity: FactInstagramCommentMetrics):
    result = bigquery_client.insert_rows_json(
      table=self.table_name,
      json_rows=[entity]
    )

    return result

