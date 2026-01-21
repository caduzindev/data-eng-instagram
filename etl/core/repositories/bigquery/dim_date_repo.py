from typing import List
from ...db.bigquery import bigquery_client

from core.entities.instagram import DimDate
from core.utils.serialize import serialize_dataclass

class DimDateRepo:
  dataset_id = "instagram_data"
  table_name = "dim_date"
    
  @property
  def table_id(self):
      return f"{self.dataset_id}.{self.table_name}"

  def save(self, entities: List[DimDate]):
    result = bigquery_client.insert_rows_json(
      table=self.table_id,
      json_rows=serialize_dataclass(entities)
    )

    return result

dim_date_repo = DimDateRepo()