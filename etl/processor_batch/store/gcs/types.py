from enum import Enum
from typing import TypedDict, Dict

class ValidExtension(Enum):
  JSON = 'json'

mime_types_map: Dict[ValidExtension, str] = {
  "json": "application/json"
}

class UploadFile:
  def __init__(self, bucket_name: str, file_name: str, extension: ValidExtension, buffer: str):
    self.bucket_name = bucket_name
    self.file_name = file_name
    self.extension = extension.value
    self.buffer = buffer
    self.mime_type = mime_types_map[extension.value]

class UploadFileReturn(TypedDict):
  saved_path: str