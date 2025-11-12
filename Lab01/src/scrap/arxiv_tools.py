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
ARXIV_CLIENT = arxiv.Client(page_size=1, delay_seconds=4, num_retries=6)
_ARXIV_GATE = threading.Semaphore(1)
ARXIV_DELAY_SEC = 1.5  # 1.5–3.0s là an toàn

def _sleep_backoff(try_idx):
    # exponential backoff có jitter: 1, 2, 4, 8, 16... (giới hạn 20s)
    base = min(20, 2 ** try_idx)
    time.sleep(base + 0.2)
@functools.lru_cache(maxsize=4096)
def get_result_by_id(arxiv_id: str) -> arxiv.Result:
    tries = 0
    while True:
        with _ARXIV_GATE:  # chặn song song
            try:
                search = arxiv.Search(id_list=[arxiv_id], max_results=1)
                return next(ARXIV_CLIENT.results(search))
            except arxiv.HTTPError as e:
                # 429/503 -> backoff rồi thử lại vài lần
                if getattr(e, "status", None) in (429, 503) or "429" in str(e) or "503" in str(e):
                    if tries < 6:
                        _sleep_backoff(tries)
                        tries += 1
                        continue
                # các lỗi khác: ném ra để caller xử lý
                raise

# @functools.lru_cache(maxsize=4096)
# def get_result_by_id(arxiv_id: str) -> arxiv.Result:
#     search = arxiv.Search(id_list=[arxiv_id])
#     return next(ARXIV_CLIENT.results(search))

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


# def try_download_source(arxiv_id_with_ver: str, save_dir: str, filename: str) -> bool:
#     """
#     Download file .tar(.gz) of a version.
#     Return True if valid tar downloaded, False otherwise.
#     Logs detailed server responses for debugging.
#     """
#     tgz_path = os.path.join(save_dir, filename)
#     try:
#         res = get_result_by_id(arxiv_id_with_ver)
#         res.download_source(dirpath=save_dir, filename=filename)

#         # Kiểm tra tar hợp lệ
#         if not is_tar_ok(tgz_path):
#             # Nếu server trả HTML/404 thay vì tar
#             if os.path.exists(tgz_path):
#                 with open(tgz_path, "rb") as f:
#                     head = f.read(256)
#                 print(f"[WARN] {arxiv_id_with_ver} => invalid tar (maybe HTML?) head={head[:80]!r}")
#                 os.remove(tgz_path)
#             return False

#         return True

#     except HTTPError as e:
#         print(f"[HTTPError] {arxiv_id_with_ver} => status={e.status_code} url={e.url}")
#         # 429: rate-limit; 404: no source; 403: access denied
#         if e.status_code == 429:
#             print("→ Too many requests: hit rate limit, try sleep/backoff.")
#         elif e.status_code == 404:
#             print("→ No source for this paper/version.")
#         elif e.status_code == 403:
#             print("→ Forbidden: requester-pays or IP blocked.")
#         else:
#             print(traceback.format_exc())
#         return False

#     except requests.HTTPError as e:
#         print(f"[requests.HTTPError] {arxiv_id_with_ver} => {e.response.status_code} {e.response.reason}")
#         print("Response body preview:", e.response.text[:300])
#         return False

#     except Exception as e:
#         print(f"[EXCEPTION] {arxiv_id_with_ver} => {type(e).__name__}: {e}")
#         print(traceback.format_exc())
#         if os.path.exists(tgz_path):
#             try: os.remove(tgz_path)
#             except: pass
#         return False


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
        print(traceback.format_exc())
        if os.path.exists(tgz_path):
            try: os.remove(tgz_path)
            except: pass
        return False
def try_download_source(arxiv_id_with_ver: str, save_dir: str, filename: str) -> bool:
    """
    Download .tar(.gz) của 1 version.
    - Thử lib arxiv nếu có pdf_url
    - Nếu pdf_url=None hoặc lỗi => fallback sang e-print
    """
    tgz_path = os.path.join(save_dir, filename)
    try:
        res = get_result_by_id(arxiv_id_with_ver)

        # Guard: tránh AttributeError khi pdf_url None
        if getattr(res, "pdf_url", None):
            try:
                res.download_source(dirpath=save_dir, filename=filename)
            except Exception as e:
                # Lib fail → thử e-print
                ok, why = _download_via_eprint(arxiv_id_with_ver, tgz_path)
                if not ok:
                    print(f"[WARN] {arxiv_id_with_ver} e-print fallback failed: {why}")
                    if os.path.exists(tgz_path):
                        try: os.remove(tgz_path)
                        except: pass
                    return False
        else:
            # Không có pdf_url ⇒ đi thẳng e-print
            ok, why = _download_via_eprint(arxiv_id_with_ver, tgz_path)
            if not ok:
                print(f"[WARN] {arxiv_id_with_ver} e-print failed: {why}")
                if os.path.exists(tgz_path):
                    try: os.remove(tgz_path)
                    except: pass
                return False

        # Kiểm tra tar hợp lệ
        if not is_tar_ok(tgz_path):
            try:
                # đọc vài byte đầu để debug
                with open(tgz_path, "rb") as f:
                    head = f.read(128)
                print(f"[WARN] {arxiv_id_with_ver} invalid tar (head={head!r}); deleting.")
                os.remove(tgz_path)
            except Exception:
                pass
            return False

        return True

    except Exception as e:
        print(f"[EXCEPTION] {arxiv_id_with_ver}: {type(e).__name__}: {e}")
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
