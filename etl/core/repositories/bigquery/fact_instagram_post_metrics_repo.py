from ...db.bigquery import bigquery_client
from core.utils.serialize import serialize_dataclass
from core.entities.instagram import FactInstagramPostMetrics
from typing import List

class FactInstagramPostMetricsRepo:
  dataset_id = "instagram_data"
  table_name = "fact_instagram_post_metrics"
    
  @property
  def table_id(self):
      return f"{self.dataset_id}.{self.table_name}"

  def save(self, entities: List[FactInstagramPostMetrics]):
    result = bigquery_client.insert_rows_json(
      table=self.table_id,
      json_rows=serialize_dataclass(entities)
    )

    return result

fact_instagram_post_metrics_repo = FactInstagramPostMetricsRepo()