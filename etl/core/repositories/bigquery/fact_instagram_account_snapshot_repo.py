from ...db.bigquery import bigquery_client
from core.utils.serialize import serialize_dataclass
from typing import List
from core.entities.instagram import FactInstagramAccountSnapshot

class FactInstagramAccountSnapshotRepo:
  dataset_id = "instagram_data"
  table_name = "fact_instagram_account_snapshot"
    
  @property
  def table_id(self):
      return f"{self.dataset_id}.{self.table_name}"

  def save(self, entities: List[FactInstagramAccountSnapshot]):
    result = bigquery_client.insert_rows_json(
      table=self.table_id,
      json_rows=serialize_dataclass(entities)
    )

    return result

fact_instagram_account_snapshot_repo = FactInstagramAccountSnapshotRepo()