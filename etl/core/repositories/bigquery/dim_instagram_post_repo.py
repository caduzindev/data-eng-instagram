from typing import List
from ...db.bigquery import bigquery_client
from core.utils.serialize import serialize_dataclass
from etl.core.entities.instagram import DimInstagramPost

class DimInstagramPostRepo:
  table_name = "dim_instagram_post"

  def save(self, entities: List[DimInstagramPost]):
    result = bigquery_client.insert_rows_json(
      table=self.table_name,
      json_rows=[serialize_dataclass(entities)]
    )

    return result

dim_instagram_post_repo = DimInstagramPostRepo()