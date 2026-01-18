from ...db.bigquery import bigquery_client
from core.utils.serialize import serialize_dataclass
from etl.core.entities.instagram import FactInstagramPostMetrics
from typing import List

class FactInstagramPostMetricsRepo:
  table_name = "fact_instagram_post_metrics"

  def save(self, entities: List[FactInstagramPostMetrics]):
    result = bigquery_client.insert_rows_json(
      table=self.table_name,
      json_rows=[serialize_dataclass(entities)]
    )

    return result

fact_instagram_post_metrics_repo = FactInstagramPostMetricsRepo()