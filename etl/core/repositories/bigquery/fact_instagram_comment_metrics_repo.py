from ...db.bigquery import bigquery_client

from etl.core.entities.instagram import FactInstagramCommentMetrics
from core.utils.serialize import serialize_dataclass
from typing import List
class FactInstagramCommentMetricsRepo:
  table_name = "fact_instagram_comment_metrics"

  def save(self, entities: List[FactInstagramCommentMetrics]):
    result = bigquery_client.insert_rows_json(
      table=self.table_name,
      json_rows=[serialize_dataclass(entities)]
    )

    return result

fact_instagram_comment_metrics_repo = FactInstagramCommentMetricsRepo()