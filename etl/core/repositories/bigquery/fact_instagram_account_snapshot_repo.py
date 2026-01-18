from ...db.bigquery import bigquery_client
from core.utils.serialize import serialize_dataclass
from typing import List
from etl.core.entities.instagram import FactInstagramAccountSnapshot

class FactInstagramAccountSnapshotRepo:
  table_name = "fact_instagram_account_snapshot"

  def save(self, entities: List[FactInstagramAccountSnapshot]):
    result = bigquery_client.insert_rows_json(
      table=self.table_name,
      json_rows=[serialize_dataclass(entities)]
    )

    return result

fact_instagram_account_snapshot_repo = FactInstagramAccountSnapshotRepo()