from ...db.bigquery import bigquery_client

from etl.core.entities.instagram import FactInstagramAccountSnapshot

class FactInstagramAccountSnapshotRepo:
  table_name = "fact_instagram_account_snapshot"

  def save(self, entity: FactInstagramAccountSnapshot):
    result = bigquery_client.insert_rows_json(
      table=self.table_name,
      json_rows=[entity]
    )

    return result

