import json
from datetime import datetime
from pathlib import Path

def utc_now():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def save_json(obj, path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def load_json(path):
    if not Path(path).exists():
        return None
    with open(path) as f:
        return json.load(f)
