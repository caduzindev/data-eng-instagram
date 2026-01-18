from typing import List
from ...db.bigquery import bigquery_client

from etl.core.entities.instagram import DimInstagramComment
from core.utils.serialize import serialize_dataclass
class DimInstagramCommentRepo:
  table_name = "dim_instagram_comment"

  def save(self, entities: List[DimInstagramComment]):
    result = bigquery_client.insert_rows_json(
      table=self.table_name,
      json_rows=[serialize_dataclass(entities)]
    )

    return result

dim_instagram_comment_repo = DimInstagramCommentRepo()