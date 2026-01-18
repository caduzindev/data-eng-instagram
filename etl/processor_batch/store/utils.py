import uuid
from datetime import datetime, timezone
from typing import Any

def generate_unique_filename(prefix: str, ext: str):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")
    uid = uuid.uuid4()
    return f"{prefix}_{timestamp}_{uid}.{ext}"

def safe_get(data: Any, *keys, default=None):
    current = data

    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default

    return current