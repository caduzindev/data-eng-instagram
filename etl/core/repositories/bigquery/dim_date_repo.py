from ...db.bigquery import bigquery_client

from etl.core.entities.instagram import DimDate

class DimDateRepo:
  table_name = "dim_date"

  def save(self, entity: DimDate):
    result = bigquery_client.insert_rows_json(
      table=self.table_name,
      json_rows=[entity]
    )

    return result

