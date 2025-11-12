import os
import time
import tarfile
import shutil
import threading
import traceback
import requests
import random

from dataclasses import dataclass
from typing import List, Optional
import arxiv
from arxiv import HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
import functools

from .utils import ensure_dir, to_yymm_id, fetch

# =========================
# Config
# =========================
TEX_BIB_EXTS = {".tex", ".bib"}

ARXIV_CLIENT = arxiv.Client(page_size=1, delay_seconds=4, num_retries=6)
_ARXIV_GATE = threading.Semaphore(1)   # serialize metadata calls
ARXIV_DELAY_SEC = 1.5                  # delay mềm giữa 2 metadata calls
_LAST_META_CALL_TS = 0.0               # mốc thời gian lần meta call gần nhất

UA = "Mozilla/5.0 (compatible; arxiv-crawler/1.0)"
TIMEOUT = 30


# =========================
# Utilities
# =========================
def _sleep_backoff(try_idx: int):
    """Exponential backoff + jitter."""
    base = min(20, 2 ** try_idx)
    time.sleep(base + random.uniform(0.0, 0.4))


def is_tar_ok(path: str) -> bool:
    """Tệp tồn tại, không phải HTML, mở được như tar."""
    if not os.path.exists(path):
        return False
    try:
        with open(path, "rb") as f:
            head = f.read(1024).lower()
            if b"<html" in head or b"<!doctype html" in head:
                return False
        with tarfile.open(path, "r:*") as tar:
            _ = tar.getmembers()
        return True
    except (tarfile.TarError, OSError, EOFError):
        return False


# =========================
# arXiv metadata (có gate + delay mềm)
# =========================
@functools.lru_cache(maxsize=4096)
def get_result_by_id(arxiv_id: str) -> arxiv.Result:
    global _LAST_META_CALL_TS
    tries = 0
    while True:
        with _ARXIV_GATE:
            # delay mềm giữa các lần gọi export.arxiv.org
            now = time.time()
            wait = ARXIV_DELAY_SEC - (now - _LAST_META_CALL_TS)
            if wait > 0:
                time.sleep(wait)

            try:
                search = arxiv.Search(id_list=[arxiv_id], max_results=1)
                res = next(ARXIV_CLIENT.results(search))
                _LAST_META_CALL_TS = time.time()
                return res
            except arxiv.HTTPError as e:
                status = getattr(e, "status_code", None)
                if status in (429, 503) or "429" in str(e) or "503" in str(e):
                    if tries < 6:
                        _sleep_backoff(tries)
                        tries += 1
                        continue
                raise


def list_all_versions(base_id: str, v1_only: bool = False) -> List[int]:
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


# =========================
# Download source (ưu tiên e-print, fallback API)
# =========================
def _download_via_eprint(arxiv_id_with_ver: str, out_path: str):
    """
    Tải trực tiếp từ e-print: https://arxiv.org/e-print/{idv}
    Stream -> tránh seek, kiểm tra HTML đầu.
    Trả (ok: bool, why: str)
    """
    url = f"https://arxiv.org/e-print/{arxiv_id_with_ver}"
    try:
        with requests.get(url, stream=True, timeout=TIMEOUT, headers={"User-Agent": UA}) as r:
            status = r.status_code
            ctype = r.headers.get("Content-Type", "")
            dispo = r.headers.get("Content-Disposition", "")

            if status != 200:
                return False, f"HTTP {status} from {url}"

            first_bytes = b""
            wrote_any = False
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if not chunk:
                        continue
                    if not wrote_any:
                        first_bytes = chunk[:1024]
                        wrote_any = True
                    f.write(chunk)

            if not wrote_any:
                if os.path.exists(out_path):
                    try:
                        os.remove(out_path)
                    except:
                        pass
                return False, "Empty body"

            fb_lc = first_bytes.lower()
            if (b"<html" in fb_lc) or (b"<!doctype html" in fb_lc):
                if os.path.exists(out_path):
                    try:
                        os.remove(out_path)
                    except:
                        pass
                return False, f"HTML instead of tar. Content-Type={ctype}; Disposition={dispo}"

            return True, "OK"
    except Exception as e:
        return False, f"EXC {type(e).__name__}: {e}"


def try_download_source(arxiv_id_with_ver: str, save_dir: str, filename: str) -> bool:
    """
    Tải .tar(.gz) của 1 version:
      1) e-print trước
      2) nếu fail -> fallback qua lib arxiv (API)
    """
    tgz_path = os.path.join(save_dir, filename)
    os.makedirs(save_dir, exist_ok=True)

    print(f"[DL] {arxiv_id_with_ver} -> e-print...", flush=True)
    ok, why = _download_via_eprint(arxiv_id_with_ver, tgz_path)
    if ok and is_tar_ok(tgz_path):
        print(f"[OK] {arxiv_id_with_ver} via e-print", flush=True)
        return True
    if not ok:
        print(f"[WARN] {arxiv_id_with_ver} e-print failed: {why}", flush=True)
    else:
        print(f"[WARN] {arxiv_id_with_ver} e-print invalid tar; removing", flush=True)
        try:
            os.remove(tgz_path)
        except:
            pass

    print(f"[DL] {arxiv_id_with_ver} -> arxiv API fallback...", flush=True)
    try:
        res = get_result_by_id(arxiv_id_with_ver)
        res.download_source(dirpath=save_dir, filename=filename)
        if not is_tar_ok(tgz_path):
            try:
                with open(tgz_path, "rb") as f:
                    head = f.read(128)
                print(f"[WARN] {arxiv_id_with_ver} invalid tar via API (head={head!r}); deleting.", flush=True)
                os.remove(tgz_path)
            except Exception:
                pass
            return False
        print(f"[OK] {arxiv_id_with_ver} via API", flush=True)
        return True
    except Exception as e:
        print(f"[EXCEPTION] {arxiv_id_with_ver}: {type(e).__name__}: {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        if os.path.exists(tgz_path):
            try:
                os.remove(tgz_path)
            except:
                pass
        return False


# =========================
# Extract .tex / .bib
# =========================
def extract_tex_bib(tar_path: str, out_dir: str):
    ensure_dir(out_dir)
    total_files = 0
    ext_counts = {}
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

    print(
        f"[extract_tex_bib] '{os.path.basename(tar_path)}' "
        f"total_files={total_files}, "
        f"tex_extracted={ext_counts.get('.tex',0)}, "
        f"bib_extracted={ext_counts.get('.bib',0)}",
        flush=True,
    )
    return {"total_files": total_files, "ext_counts": ext_counts}

@dataclass
class PaperMeta:
    paper_title: str
    authors: List[str]
    publication_venue: Optional[str]
    submission_date: str
    revised_dates: List[str]
# --- end added ---

def build_metadata(base_id: str, versions: List[int]) -> PaperMeta:
    res_v1 = get_result_by_id(f"{base_id}v{versions[0]}")
    title = res_v1.title
    authors = [a.name for a in res_v1.authors]
    venue = res_v1.journal_ref or (res_v1.comment or None)
    revised_dates = [get_result_by_id(f"{base_id}v{v}").published.strftime("%Y-%m-%d") for v in versions]
    return PaperMeta(
        paper_title=title,
        authors=authors,
        publication_venue=venue,
        submission_date=revised_dates[0],
        revised_dates=revised_dates,
    )

# --- chuyển phần demo/ví dụ chạy vào guard để không chạy khi import ---
def process_one(arxiv_id_with_ver):
    # gọi try_download_source(...) -> lưu .tar
    # gọi extract_only_tex(tar_path, out_dir)
    # gọi save_bibtex(base_id, out_dir)
    return arxiv_id_with_ver

if __name__ == "__main__":
    # ví dụ: thay ids bằng danh sách thực khi chạy trực tiếp
    ids = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = [ex.submit(process_one, i) for i in ids]
        for f in as_completed(futures):
            print("done", f.result())
# --- end guard ---
