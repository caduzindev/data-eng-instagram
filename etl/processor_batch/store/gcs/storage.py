from google.cloud import storage
from .types import UploadFile, UploadFileReturn
from ..utils import generate_unique_filename

from core.env import CoreEnv

class StorageGCS:
  def __init__(self):
    self.client = storage.Client.from_service_account_json(CoreEnv().account_service_instagram_gcp)

  def upload_file(self, payload: UploadFile) -> UploadFileReturn:
    bucket = self.client.bucket(payload.bucket_name)
    file_name = generate_unique_filename(prefix=payload.file_name, ext=payload.extension)
    blob = bucket.blob(file_name)

    blob.upload_from_string(payload.buffer, content_type=payload.mime_type)

    print(
        f"{file_name} uploaded to {payload.bucket_name}."
    )

    return {
      'saved_path': f'{payload.bucket_name}/{file_name}'
    }

storage_gcs = StorageGCS()