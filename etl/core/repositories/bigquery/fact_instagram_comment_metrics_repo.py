from ...db.bigquery import bigquery_client

from core.entities.instagram import FactInstagramCommentMetrics
from core.utils.serialize import serialize_dataclass
from typing import List
class FactInstagramCommentMetricsRepo:
  dataset_id = "instagram_data"
  table_name = "fact_instagram_comment_metrics"
    
  @property
  def table_id(self):
      return f"{self.dataset_id}.{self.table_name}"

  def save(self, entities: List[FactInstagramCommentMetrics]):
    result = bigquery_client.insert_rows_json(
      table=self.table_id,
      json_rows=serialize_dataclass(entities)
    )

    return result

fact_instagram_comment_metrics_repo = FactInstagramCommentMetricsRepo()