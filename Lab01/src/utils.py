import os
import re
import json
import requests

HEADERS = {"User-Agent": "HCMUS-DataScience-Lab/1.0"}

def to_yymm_id(arxiv_id: str) -> str:
    """'1706.03762' -> '1706-03762'. Giữ nguyên phần 'vX' nếu có."""
    v = ""
    m = re.search(r"v(\d+)$", arxiv_id)
    if m:
        v = m.group(0)
        arxiv_id = arxiv_id[: -(len(v))]
    return arxiv_id.replace(".", "-") + v

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def write_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=4)

def fetch(url: str, params=None) -> requests.Response:
    r = requests.get(url, params=params, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r
