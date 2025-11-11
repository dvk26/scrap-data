import os, re, json, time, argparse, io, tarfile, hashlib
from datetime import datetime
from dateutil import parser as dtparser
from typing import List, Dict, Optional, Tuple
import requests
from bs4 import BeautifulSoup
import arxiv
from tqdm import tqdm

# ===================== CONFIG =====================
PAPER_TITLES = [
    "Computer vision-based food calorie estimation: dataset, method, and experiment",
    "Deep Learning-Based Food Calorie Estimation Method in Dietary Assessment",
    "Using Distance Estimation and Deep Learning to Simplify Calibration in Food Calorie Measurement",
    "Learning Daily Calorie Intake Standard using a Mobile Game",
    "Bangladeshi Street Food Calorie Estimation Using Improved YOLOv8 and Regression Model",
]
REQUESTS_TIMEOUT = 25
RATE_LIMIT_SEC = 1.0  # tránh 429
# ==================================================

def slug(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())

def title_score(a: str, b: str) -> float:
    # điểm khớp tiêu đề đơn giản (Jaccard trên từ đơn)
    sa, sb = set(re.findall(r"[a-z0-9]+", slug(a))), set(re.findall(r"[a-z0-9]+", slug(b)))
    if not sa or not sb: return 0.0
    inter = len(sa & sb); union = len(sa | sb)
    return inter / union

def arxiv_search_best_match(title: str, k: int = 20) -> Optional[arxiv.Result]:
    # Tìm nhiều kết quả, chọn tiêu đề khớp cao nhất
    search = arxiv.Search(query=f'all:"{title}"', max_results=k, sort_by=arxiv.SortCriterion.Relevance)
    client = arxiv.Client()
    best, best_sc = None, 0.0
    for res in client.results(search):
        sc = title_score(title, res.title or "")
        if sc > best_sc:
            best, best_sc = res, sc
    # Ngưỡng khớp: >= 0.45 cho tiêu đề gần giống; bạn có thể siết chặt hơn nếu muốn
    return best if best_sc >= 0.45 else None

def yyyymm_from_arxiv_id(arxiv_id: str) -> str:
    # arXiv ID kiểu mới: yymm.nnnnn → map sang yyyymm
    m = re.match(r"^(\d{2})(\d{2})\.\d{4,5}(?:v\d+)?$", arxiv_id)
    if not m:  # fallback
        return "unknown"
    yy, mm = int(m.group(1)), m.group(2)
    yyyy = 2000 + yy
    return f"{yyyy}{mm}"

def base_id(arxiv_id: str) -> str:
    return re.sub(r"v\d+$", "", arxiv_id)

def fetch_abs_html(arxiv_id: str) -> str:
    url = f"https://arxiv.org/abs/{base_id(arxiv_id)}"
    r = requests.get(url, timeout=REQUESTS_TIMEOUT)
    r.raise_for_status()
    return r.text

def parse_versions_from_abs(html: str) -> List[Tuple[int, str]]:
    """
    Trả về danh sách [(version_number, iso_datetime_str), ...]
    Dựa vào “(vN)” và mốc thời gian trong phần submission history.
    """
    soup = BeautifulSoup(html, "html.parser")
    hist = soup.find("div", {"class": "submission-history"})
    versions = []
    if not hist:
        # fallback: đếm [vN] trong body
        text = soup.get_text("\n")
        vnums = sorted({int(v) for v in re.findall(r"\bv(\d+)\b", text)})
        return [(v, "") for v in vnums] if vnums else [(1, "")]
    # mỗi dòng kiểu: [v1] Fri, 1 Sep 2023 12:34:56 UTC (123 KB)
    for line in hist.get_text("\n").splitlines():
        m = re.search(r"\[v(\d+)\].*?(\w{3},.*?UTC)", line)
        if m:
            v = int(m.group(1))
            # parse datetime
            try:
                dt = dtparser.parse(m.group(2))
                iso = dt.isoformat()
            except Exception:
                iso = ""
            versions.append((v, iso))
    if not versions:
        # ít nhất có v1
        versions = [(1, "")]
    versions.sort(key=lambda x: x[0])
    return versions

def download_source_version(arxiv_id: str, version: int, out_path: str) -> None:
    """
    Tải source LaTeX qua endpoint e-print (tarball).
    """
    base = base_id(arxiv_id)
    url = f"https://arxiv.org/e-print/{base}v{version}"
    headers = {"User-Agent": "lab1-scraper/0.1 (+student)"}
    with requests.get(url, stream=True, headers=headers, timeout=REQUESTS_TIMEOUT) as r:
        if r.status_code != 200:
            raise RuntimeError(f"Download failed {r.status_code} for {url}")
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    time.sleep(RATE_LIMIT_SEC)

def strip_images_from_tar_gz(in_path: str, out_path: str) -> None:
    """
    Loại ảnh (png/jpg/jpeg/pdf/eps/svg) khỏi source để giảm size theo yêu cầu lab.
    """
    exts = {".png", ".jpg", ".jpeg", ".pdf", ".eps", ".svg"}
    with tarfile.open(in_path, "r:gz") as tar_in:
        with tarfile.open(out_path, "w:gz") as tar_out:
            for m in tar_in.getmembers():
                name = m.name.lower()
                if any(name.endswith(e) for e in exts):
                    continue
                # sao chép file (nếu là file thường)
                if m.isfile():
                    fileobj = tar_in.extractfile(m)
                    if fileobj:
                        data = fileobj.read()
                        info = tarfile.TarInfo(name=m.name)
                        info.size = len(data)
                        tar_out.addfile(info, io.BytesIO(data))
                elif m.isdir():
                    tar_out.addfile(m)

def fetch_bibtex(arxiv_id: str) -> str:
    url = f"https://arxiv.org/bibtex/{base_id(arxiv_id)}"
    r = requests.get(url, timeout=REQUESTS_TIMEOUT)
    r.raise_for_status()
    return r.text

def get_references_semanticscholar(arxiv_id: str) -> List[Dict]:
    url = f"https://api.semanticscholar.org/graph/v1/paper/arXiv:{base_id(arxiv_id)}"
    params = {"fields": "references,references.externalIds,references.title,references.authors,references.year"}
    r = requests.get(url, params=params, timeout=REQUESTS_TIMEOUT)
    if r.status_code != 200:
        return []
    refs = r.json().get("references", []) or []
    out = []
    for ref in refs:
        ext = ref.get("externalIds") or {}
        rid = ext.get("ArXiv")  # có thể None
        out.append({
            "title": ref.get("title"),
            "authors": [a.get("name") for a in (ref.get("authors") or []) if a.get("name")],
            "arxiv_id": rid,
            "year": ref.get("year"),
        })
    time.sleep(RATE_LIMIT_SEC)
    return out

def result_to_metadata(res: arxiv.Result, versions: List[Tuple[int, str]]) -> Dict:
    title = (res.title or "").strip()
    authors = [a.name for a in res.authors]
    published = res.published.isoformat() if res.published else ""
    revised_dates = [iso for (v, iso) in versions if v > 1 and iso]
    venue = "arXiv"
    return {
        "title": title,
        "authors": authors,
        "submission_date": published,
        "revised_dates": revised_dates,
        "venue": venue,
    }

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def main(student_id: str, titles: List[str]):
    base_out = os.path.abspath(student_id)
    ensure_dir(base_out)
    not_found = []

    pbar = tqdm(titles, desc="Resolving titles → arXiv IDs")
    for title in pbar:
        pbar.set_postfix_str(title[:40] + ("..." if len(title) > 40 else ""))
        res = arxiv_search_best_match(title)
        if not res:
            not_found.append(title)
            continue

        # base id (no version), yyyymm
        short_id = res.get_short_id()  # e.g., '2310.12345'
        yyyymm = yyyymm_from_arxiv_id(short_id)
        folder_name = f"{yyyymm}-{short_id.split('.')[-1]}"
        paper_dir = os.path.join(base_out, folder_name)
        tex_dir = os.path.join(paper_dir, "tex")
        ensure_dir(tex_dir)

        # versions
        html = fetch_abs_html(short_id)
        versions = parse_versions_from_abs(html)
        if not versions:
            versions = [(1, "")]

        # download each version source & strip images
        for (vnum, _) in versions:
            raw_tgz = os.path.join(tex_dir, f"{short_id}v{vnum}.tar.gz")
            clean_tgz = os.path.join(tex_dir, f"{short_id}v{vnum}.nofigs.tar.gz")
            try:
                download_source_version(short_id, vnum, raw_tgz)
                # strip images per lab note
                strip_images_from_tar_gz(raw_tgz, clean_tgz)
                try:
                    os.remove(raw_tgz)  # chỉ giữ bản đã lọc ảnh
                except Exception:
                    pass
            except Exception as e:
                print(f"[WARN] Download v{vnum} failed for {short_id}: {e}")

        # save BibTeX
        try:
            bib = fetch_bibtex(short_id)
            with open(os.path.join(paper_dir, "references.bib"), "w", encoding="utf-8") as f:
                f.write(bib)
        except Exception as e:
            print(f"[WARN] BibTeX failed for {short_id}: {e}")

        # metadata.json
        meta = result_to_metadata(res, versions)
        with open(os.path.join(paper_dir, "metadata.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        # references.json (map arXiv id (yyyymm-id) → metadata cơ bản)
        refs = get_references_semanticscholar(short_id)
        refs_map = {}
        for r in refs:
            rid = r.get("arxiv_id")
            if not rid:
                continue
            yyyymm_ref = yyyymm_from_arxiv_id(rid)
            key = f"{yyyymm_ref}-{rid.split('.')[-1]}"
            refs_map[key] = {
                "title": r.get("title"),
                "authors": r.get("authors") or [],
                "submission_date": "",  # có thể mở rộng: query thêm nếu cần
                "revised_dates": []
            }
        with open(os.path.join(paper_dir, "references.json"), "w", encoding="utf-8") as f:
            json.dump(refs_map, f, ensure_ascii=False, indent=2)

        time.sleep(RATE_LIMIT_SEC)

    if not_found:
        with open(os.path.join(base_out, "not_on_arxiv.txt"), "w", encoding="utf-8") as f:
            for t in not_found:
                f.write(t + "\n")
        print(f"[INFO] {len(not_found)} titles not found on arXiv. See not_on_arxiv.txt")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--student-id", required=True, help="Your student id, used as root folder name")
    args = ap.parse_args()
    main(args.student_id, PAPER_TITLES)
