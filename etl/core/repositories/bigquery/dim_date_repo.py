from typing import List
from ...db.bigquery import bigquery_client

from etl.core.entities.instagram import DimDate
from core.utils.serialize import serialize_dataclass

class DimDateRepo:
  table_name = "dim_date"

  def save(self, entities: List[DimDate]):
    result = bigquery_client.insert_rows_json(
      table=self.table_name,
      json_rows=[serialize_dataclass(entities)]
    )

    return result

dim_date_repo = DimDateRepo()