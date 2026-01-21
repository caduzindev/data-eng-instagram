from typing import List
from ...db.bigquery import bigquery_client

from core.entities.instagram import DimInstagramComment
from core.utils.serialize import serialize_dataclass
class DimInstagramCommentRepo:
  dataset_id = "instagram_data"
  table_name = "dim_instagram_comment"
    
  @property
  def table_id(self):
      return f"{self.dataset_id}.{self.table_name}"

  def save(self, entities: List[DimInstagramComment]):
    result = bigquery_client.insert_rows_json(
      table=self.table_id,
      json_rows=serialize_dataclass(entities)
    )

    return result

dim_instagram_comment_repo = DimInstagramCommentRepo()