# scrap/arxiv_tools.py
import os
import tarfile
import shutil
import time
import random
import threading
import functools
from dataclasses import dataclass
import traceback
from typing import List, Optional

import arxiv
import requests

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

UA = "Mozilla/5.0 (compatible; arxiv-crawler/1.0)"
TIMEOUT = 30

def _download_via_eprint(arxiv_id_with_ver: str, out_path: str) -> tuple[bool, str]:
    """
    Tải trực tiếp từ e-print endpoint: https://arxiv.org/e-print/{idv}
    - Không dùng seek trên stream
    - Buffer vài trăm byte đầu để phát hiện HTML
    """
    url = f"https://arxiv.org/e-print/{arxiv_id_with_ver}"
    try:
        with requests.get(url, stream=True, timeout=TIMEOUT, headers={"User-Agent": UA}) as r:
            status = r.status_code
            ctype  = r.headers.get("Content-Type", "")
            dispo  = r.headers.get("Content-Disposition", "")

            if status != 200:
                return False, f"HTTP {status} from {url}"

            # đọc stream theo chunk, vừa ghi file vừa giữ 1 buffer đầu
            first_bytes = b""
            wrote_any = False
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if not chunk:
                        continue
                    if not wrote_any:
                        # lấy ~1KB đầu để check HTML
                        take = min(len(chunk), 1024)
                        first_bytes = chunk[:take]
                        wrote_any = True
                    f.write(chunk)

            # nếu không ghi được gì → coi như fail
            if not wrote_any:
                if os.path.exists(out_path):
                    try: os.remove(out_path)
                    except: pass
                return False, "Empty body"

            # phát hiện trả về HTML (trang báo lỗi 200)
            fb_lc = first_bytes.lower()
            if (b"<html" in fb_lc) or (b"<!doctype html" in fb_lc):
                # đọc ít nội dung text để log
                # (không reopen file lớn; chỉ log dựa trên header & content-type)
                if os.path.exists(out_path):
                    try: os.remove(out_path)
                    except: pass
                return False, f"HTML instead of tar. Content-Type={ctype}; Disposition={dispo}"

            # heuristic nhanh: nếu header nói tar/gzip thì OK, còn lại để is_tar_ok kiểm tra
            looks_like_tar = ("tar" in ctype) or ("gzip" in ctype) or (".tar" in dispo) or (".gz" in dispo)
            # không bắt buộc phải true ở đây; cứ trả OK để bước sau is_tar_ok quyết định
            return True, "OK"

    except Exception as e:
        # không còn seek nên sẽ không dính UnsupportedOperation nữa
        return False, f"EXC {type(e).__name__}: {e}"

def try_download_source(arxiv_id_with_ver: str, save_dir: str, filename: str) -> bool:
    tgz_path = os.path.join(save_dir, filename)
    os.makedirs(save_dir, exist_ok=True)

    # 1) ƯU TIÊN E-PRINT
    ok, why = _download_via_eprint(arxiv_id_with_ver, tgz_path)
    if ok and is_tar_ok(tgz_path):
        return True
    if not ok:
        print(f"[WARN] {arxiv_id_with_ver} e-print failed: {why}")
    else:
        print(f"[WARN] {arxiv_id_with_ver} e-print invalid tar; removing")
        try: os.remove(tgz_path)
        except: pass

    # 2) FALLBACK: LIB ARXIV (đi qua gate + backoff)
    try:
        res = get_result_by_id(arxiv_id_with_ver)
        res.download_source(dirpath=save_dir, filename=filename)
        if not is_tar_ok(tgz_path):
            try:
                with open(tgz_path, "rb") as f:
                    head = f.read(128)
                print(f"[WARN] {arxiv_id_with_ver} invalid tar via API (head={head!r}); deleting.")
                os.remove(tgz_path)
            except Exception:
                pass
            return False
        return True
    except Exception as e:
        print(f"[EXCEPTION] {arxiv_id_with_ver}: {type(e).__name__}: {e}")
        if os.path.exists(tgz_path):
            try: os.remove(tgz_path)
            except: pass
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
