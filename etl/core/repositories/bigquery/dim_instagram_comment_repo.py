from ...db.bigquery import bigquery_client

from etl.core.entities.instagram import DimInstagramComment

class DimInstagramCommentRepo:
  table_name = "dim_instagram_comment"

  def save(self, entity: DimInstagramComment):
    result = bigquery_client.insert_rows_json(
      table=self.table_name,
      json_rows=[entity]
    )

    return result

