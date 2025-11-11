import threading
import time
from tqdm import tqdm
from typing import List, Dict
from .utils import fetch, to_yymm_id
from .arxiv_tools import get_result_by_id

SEM_SCHOLAR_BASE = "https://api.semanticscholar.org/graph/v1/paper/arXiv:{arxiv_id}"
SEM_DELAY_SEC = 1.2
_ss_gate = threading.Semaphore(1)

def get_references_with_arxiv_ids(base_id: str) -> List[Dict]:
    url = SEM_SCHOLAR_BASE.format(arxiv_id=base_id)
    params = {
        "fields": "references,references.externalIds,references.title,references.authors,references.paperId"
    }
    with _ss_gate:
        time.sleep(SEM_DELAY_SEC)
        data = fetch(url, params=params).json()
    refs = []
    for ref in data.get("references", []):
        ext = (ref or {}).get("externalIds") or {}
        aid = ext.get("ArXiv")
        if not aid:
            continue
        title = ref.get("title", "")
        authors = [a.get("name", "") for a in ref.get("authors", []) if a]
        refs.append({
            "arxiv_id": aid,
            "title": title,
            "authors": authors,
            "s2id": ref.get("paperId")
        })
    return refs

def enrich_references_with_dates(refs: List[Dict]) -> Dict[str, Dict]:
    out = {}
    for r in tqdm(refs, desc="Fetching referenced arXiv metadata"):
        aid = r["arxiv_id"]
        base = aid.split("v")[0]
        try:
            res_v1 = get_result_by_id(f"{base}v1")
            yymm = to_yymm_id(base)
            out[yymm] = {
                "paper_title": r["title"],
                "authors": r["authors"],
                "submission_date": res_v1.published.strftime("%Y-%m-%d"),
                "semantic_scholar_id": r["s2id"],
            }
        except Exception:
            continue
    return out
