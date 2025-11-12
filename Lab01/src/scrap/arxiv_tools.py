# scrap/arxiv_tools.py
import os
import tarfile
import shutil
import time
import random
import threading
import functools
from dataclasses import dataclass
from typing import List, Optional

import arxiv

from .utils import ensure_dir

TEX_BIB_EXTS = {".tex", ".bib"}

# ============ GLOBAL RATE LIMITER (>= 3s/request) ============
class RateLimiter:
    def __init__(self, min_interval: float = 3.5):
        self.min_interval = float(min_interval)
        self._lock = threading.Lock()
        self._last = 0.0

    def acquire(self):
        with self._lock:
            now = time.time()
            delta = now - self._last
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)
            self._last = time.time()

RATE_LIMITER = RateLimiter(3.5)

# ============ arXiv CLIENT ============
# Không dùng delay/num_retries nội bộ của lib, ta tự kiểm soát để chủ động backoff
ARXIV_CLIENT = arxiv.Client(
    page_size=50,
    delay=0,
    num_retries=0,
)

# ============ UTILITIES ============
def is_tar_ok(path: str) -> bool:
    """Quick validation: file exists, not an HTML error page, and tarfile can be opened."""
    if not os.path.exists(path):
        return False
    try:
        # fast reject: HTML error page
        with open(path, "rb") as f:
            head = f.read(1024).lower()
            if b"<html" in head or b"<!doctype html" in head:
                return False
        # try open as tar (handles .tar and .tar.gz via "r:*")
        with tarfile.open(path, "r:*") as tar:
            tar.getmembers()
        return True
    except (tarfile.TarError, OSError, EOFError):
        return False

# ============ CORE API CALLS WITH RATE LIMIT + BACKOFF ============
def _backoff_sleep(attempt: int) -> float:
    # 1, 2, 4, 8, 16, 32 ... capped @ 60 + jitter
    backoff = min(60, 2 ** attempt) + random.uniform(0.0, 1.5)
    time.sleep(backoff)
    return backoff

@functools.lru_cache(maxsize=4096)
def get_result_by_id(arxiv_id: str) -> arxiv.Result:
    """
    Lấy metadata theo id (có version) với rate-limit toàn cục + retry/backoff cho 429/503.
    Dùng lru_cache để tránh gọi lại cùng một id.
    """
    retries = 6
    for attempt in range(retries):
        try:
            RATE_LIMITER.acquire()
            search = arxiv.Search(id_list=[arxiv_id])
            return next(ARXIV_CLIENT.results(search))
        except arxiv.HTTPError as e:
            status = getattr(e, "status", None)
            if status in (429, 503):
                waited = _backoff_sleep(attempt)
                print(f"[WARN] arXiv HTTP {status} for {arxiv_id}. Backoff {waited:.1f}s (attempt {attempt+1}/{retries})")
                continue
            # Các lỗi khác: raise luôn
            raise
        except StopIteration:
            raise ValueError(f"arXiv ID not found: {arxiv_id}")
    raise RuntimeError(f"Failed to fetch {arxiv_id} after {retries} retries")

def list_all_versions(base_id: str, v1_only: bool=False) -> List[int]:
    if v1_only:
        return [1]
    versions, v = [], 1
    while True:
        try:
            _ = get_result_by_id(f"{base_id}v{v}")
            versions.append(v)
            v += 1
        except Exception:
            break
    return versions or [1]

def try_download_source(arxiv_id_with_ver: str, save_dir: str, filename: str) -> bool:
    """
    Download .tar(.gz) of a version. Return True if ok, False if not exist/invalid.
    Bọc rate-limit + backoff quanh download_source vì đó cũng là HTTP request.
    """
    tgz_path = os.path.join(save_dir, filename)
    retries = 5
    for attempt in range(retries):
        try:
            # 1) Lấy result (có cache + rate-limit)
            res = get_result_by_id(arxiv_id_with_ver)

            # 2) Tải source (cũng phải rate-limit)
            RATE_LIMITER.acquire()
            res.download_source(dirpath=save_dir, filename=filename)

            # 3) Validate file
            if not is_tar_ok(tgz_path):
                try:
                    os.remove(tgz_path)
                except Exception:
                    pass
                # Có thể là HTML/404, coi như không có source
                return False
            return True

        except arxiv.HTTPError as e:
            status = getattr(e, "status", None)
            if status in (429, 503):
                waited = _backoff_sleep(attempt)
                print(f"[WARN] arXiv HTTP {status} on download for {arxiv_id_with_ver}. Backoff {waited:.1f}s (attempt {attempt+1}/{retries})")
                continue
            # Không phải lỗi tạm → coi như không có source (để pipeline tiếp tục)
            print(f"[WARN] download_source failed for {arxiv_id_with_ver}: {e}")
            break
        except Exception as e:
            print(f"[WARN] download_source unexpected error for {arxiv_id_with_ver}: {e}")
            break

    # Cleanup nếu có file rác
    try:
        if os.path.exists(tgz_path):
            os.remove(tgz_path)
    except Exception:
        pass
    return False

def extract_tex_bib(tar_path: str, out_dir: str):
    ensure_dir(out_dir)
    total_files = 0
    ext_counts = {}
    try:
        with tarfile.open(tar_path, "r:*") as tar:
            for m in tar.getmembers():
                if not m.isfile():
                    continue
                base = os.path.basename(m.name).replace("\x00", "")
                if not base:
                    continue
                total_files += 1
                _, ext = os.path.splitext(base)
                ext = ext.lower() if ext else "<no_ext>"
                ext_counts[ext] = ext_counts.get(ext, 0) + 1

                if ext in TEX_BIB_EXTS:
                    member_f = tar.extractfile(m)
                    if member_f:
                        out_path = os.path.join(out_dir, base)
                        with open(out_path, "wb") as f:
                            shutil.copyfileobj(member_f, f)
        print(f"[extract_tex_bib] '{os.path.basename(tar_path)}' total_files={total_files}, "
              f"tex_extracted={ext_counts.get('.tex',0)}, bib_extracted={ext_counts.get('.bib',0)}")
        return {"total_files": total_files, "ext_counts": ext_counts}
    except tarfile.ReadError:
        raise ValueError("Downloaded file is not a valid tar archive")

# ============ METADATA ============

@dataclass
class PaperMeta:
    paper_title: str
    authors: List[str]
    publication_venue: Optional[str]
    submission_date: str
    revised_dates: List[str]

def build_metadata(base_id: str, versions: List[int]) -> PaperMeta:
    # v1
    res_v1 = get_result_by_id(f"{base_id}v{versions[0]}")
    title = res_v1.title
    authors = [a.name for a in res_v1.authors]
    venue = res_v1.journal_ref or (res_v1.comment or None)

    # timestamps cho từng version
    revised_dates: List[str] = []
    for v in versions:
        res_v = get_result_by_id(f"{base_id}v{v}")
        revised_dates.append(res_v.published.strftime("%Y-%m-%d"))

    return PaperMeta(
        paper_title=title,
        authors=authors,
        publication_venue=venue,
        submission_date=revised_dates[0],
        revised_dates=revised_dates,
    )
