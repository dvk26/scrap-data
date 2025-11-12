import os
import tarfile
from dataclasses import dataclass, asdict
from typing import List, Optional
import arxiv
from .utils import ensure_dir, to_yymm_id, fetch
from concurrent.futures import ThreadPoolExecutor, as_completed
import functools
TEX_BIB_EXTS = {".tex", ".bib"}
import time
import tarfile
import os
import shutil
import threading
import os, requests, traceback
from arxiv import HTTPError

# reuse a single arXiv client to reduce creation overhead
ARXIV_CLIENT = arxiv.Client(num_retries=5)  # tăng retry
_ARXIV_GATE = threading.Semaphore(1)
ARXIV_DELAY_SEC = 1.5  # 1.5–3.0s là an toàn

@functools.lru_cache(maxsize=4096)
def get_result_by_id(arxiv_id: str) -> arxiv.Result:
    with _ARXIV_GATE:
        time.sleep(ARXIV_DELAY_SEC)
        search = arxiv.Search(id_list=[arxiv_id])
        return next(ARXIV_CLIENT.results(search))

@functools.lru_cache(maxsize=4096)
def get_result_by_id(arxiv_id: str) -> arxiv.Result:
    search = arxiv.Search(id_list=[arxiv_id])
    return next(ARXIV_CLIENT.results(search))

def list_all_versions(base_id: str, v1_only: bool=False) -> List[int]:
    if v1_only:
        return [1]
    versions, v = [], 1
    while True:
        try:
            _ = get_result_by_id(f"{base_id}v{v}")
            versions.append(v); v += 1
        except Exception:
            break
    return versions or [1]


# ...existing code...
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
            # attempt to read members (will raise if corrupted)
            tar.getmembers()
        return True
    except (tarfile.TarError, OSError, EOFError):
        return False
# ...existing code...


def try_download_source(arxiv_id_with_ver: str, save_dir: str, filename: str) -> bool:
    """
    Download file .tar(.gz) of a version.
    Return True if valid tar downloaded, False otherwise.
    Logs detailed server responses for debugging.
    """
    tgz_path = os.path.join(save_dir, filename)
    try:
        res = get_result_by_id(arxiv_id_with_ver)
        res.download_source(dirpath=save_dir, filename=filename)

        # Kiểm tra tar hợp lệ
        if not is_tar_ok(tgz_path):
            # Nếu server trả HTML/404 thay vì tar
            if os.path.exists(tgz_path):
                with open(tgz_path, "rb") as f:
                    head = f.read(256)
                print(f"[WARN] {arxiv_id_with_ver} => invalid tar (maybe HTML?) head={head[:80]!r}")
                os.remove(tgz_path)
            return False

        return True

    except HTTPError as e:
        print(f"[HTTPError] {arxiv_id_with_ver} => status={e.status_code} url={e.url}")
        # 429: rate-limit; 404: no source; 403: access denied
        if e.status_code == 429:
            print("→ Too many requests: hit rate limit, try sleep/backoff.")
        elif e.status_code == 404:
            print("→ No source for this paper/version.")
        elif e.status_code == 403:
            print("→ Forbidden: requester-pays or IP blocked.")
        else:
            print(traceback.format_exc())
        return False

    except requests.HTTPError as e:
        print(f"[requests.HTTPError] {arxiv_id_with_ver} => {e.response.status_code} {e.response.reason}")
        print("Response body preview:", e.response.text[:300])
        return False

    except Exception as e:
        print(f"[EXCEPTION] {arxiv_id_with_ver} => {type(e).__name__}: {e}")
        print(traceback.format_exc())
        if os.path.exists(tgz_path):
            try: os.remove(tgz_path)
            except: pass
        return False


def extract_tex_bib(tar_path: str, out_dir: str):
    ensure_dir(out_dir)
    total_files = 0
    ext_counts = {}
    # Dùng auto detect nén: "r:*" để xử lý .tar hoặc .tar.gz
    try:
        with tarfile.open(tar_path, "r:*") as tar:
            for m in tar.getmembers():
                if not m.isfile():
                    continue
                # lấy tên file an toàn rồi tính extension
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
                        # stream copy -> không load toàn bộ nội dung vào RAM
                        with open(out_path, "wb") as f:
                            shutil.copyfileobj(member_f, f)
            # in tóm tắt sau khi duyệt xong (hữu ích để debug/monitor)
            print(f"[extract_tex_bib] '{os.path.basename(tar_path)}' total_files={total_files}, tex_extracted={ext_counts.get('.tex',0)}, bib_extracted={ext_counts.get('.bib',0)}")
            # trả về thống kê nếu caller cần dùng
            return {"total_files": total_files, "ext_counts": ext_counts}
    except tarfile.ReadError:
        # Không phải tar hợp lệ -> bỏ qua
        raise ValueError("Downloaded file is not a valid tar archive")

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
