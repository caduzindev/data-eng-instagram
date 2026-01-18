from ...db.bigquery import bigquery_client

from etl.core.entities.instagram import DimInstagramAccount

class DimInstagramAccountRepo:
  table_name = "dim_instagram_account"

  def save(self, entity: DimInstagramAccount):
    result = bigquery_client.insert_rows_json(
      table=self.table_name,
      json_rows=[entity]
    )

    return result