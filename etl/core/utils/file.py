import uuid
from datetime import datetime, timezone

def generate_unique_filename(prefix: str, ext: str):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")
    uid = uuid.uuid4()
    return f"{prefix}_{timestamp}_{uid}.{ext}"