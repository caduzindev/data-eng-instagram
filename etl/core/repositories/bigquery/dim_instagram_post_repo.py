from ...db.bigquery import bigquery_client

from etl.core.entities.instagram import DimInstagramPost

class DimInstagramPostRepo:
  table_name = "dim_instagram_post"

  def save(self, entity: DimInstagramPost):
    result = bigquery_client.insert_rows_json(
      table=self.table_name,
      json_rows=[entity]
    )

    return result

