from typing import List
from ...db.bigquery import bigquery_client
from core.utils.serialize import serialize_dataclass
from etl.core.entities.instagram import DimInstagramAccount

class DimInstagramAccountRepo:
  table_name = "dim_instagram_account"

  def save(self, entities: List[DimInstagramAccount]):
    result = bigquery_client.insert_rows_json(
      table=self.table_name,
      json_rows=[serialize_dataclass(entities)]
    )

    return result

dim_instagram_account_repo = DimInstagramAccountRepo()