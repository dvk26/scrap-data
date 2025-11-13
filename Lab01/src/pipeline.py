import os
from dataclasses import asdict
from .utils import ensure_dir, to_yymm_id, write_json
from .arxiv_tools import (
    list_all_versions, try_download_source, extract_tex_bib,
    build_metadata
)
from .utils import ensure_dir, to_yymm_id, write_json
from .semantic_scholar import (
    get_references_with_arxiv_ids, enrich_references_with_dates
)

# def process_one_paper(student_root: str, base_id: str, v1_only: bool=False, skip_ref: bool=False):
#     yymm = to_yymm_id(base_id)
#     paper_dir = os.path.join(student_root, yymm)
#     ensure_dir(paper_dir)

#     versions = list_all_versions(base_id)
#     tex_root = os.path.join(paper_dir, "tex")
#     tmp_dir  = os.path.join(paper_dir, "_tmp")
#     ensure_dir(tex_root); ensure_dir(tmp_dir)

#     any_source_ok = False

#     for v in versions:
#         arxiv_id_v = f"{base_id}v{v}"
#         tgz_name   = f"{to_yymm_id(base_id)}v{v}.tar.gz"
#         tgz_path   = os.path.join(tmp_dir, tgz_name)

#         ok = try_download_source(arxiv_id_v, tmp_dir, tgz_name)
#         if not ok:
#             print(f"[WARN] No source for {arxiv_id_v} -> skip this version")
#             continue

#         out_dir = os.path.join(tex_root, f"{to_yymm_id(base_id)}v{v}")
#         try:
#             extract_tex_bib(tgz_path, out_dir)
#             any_source_ok = True
#         except ValueError:
#             print(f"[WARN] Invalid tar for {arxiv_id_v} -> skip extraction")
#             # xóa file rác nếu cần
#             try:
#                 os.remove(tgz_path)
#             except Exception:
#                 pass
#             continue

#     # Nếu không có version nào có source -> vẫn ghi metadata/bib/ref (nếu bạn muốn)
#     # hoặc có thể chọn bỏ hẳn paper này bằng return sớm:
#     if not any_source_ok:
#         print(f"[INFO] {base_id}: no TeX sources available -> continue with metadata/bib/references")

#     # metadata.json
#     meta = build_metadata(base_id, versions)
#     write_json(os.path.join(paper_dir, "metadata.json"), asdict(meta))

#     # references.bib
#     # try:
#     #     bib = fetch_bibtex(base_id)
#     #     with open(os.path.join(paper_dir, "references.bib"), "w", encoding="utf-8") as f:
#     #         f.write(bib)
#     # except Exception:
#     #     pass

#     # references.json
#     try:
#         refs = get_references_with_arxiv_ids(base_id)
#         refs_map = enrich_references_with_dates(refs)
#         write_json(os.path.join(paper_dir, "references.json"), refs_map)
#     except Exception:
#         write_json(os.path.join(paper_dir, "references.json"), {})

#     # cleanup
#     for fn in os.listdir(tmp_dir):
#         try:
#             os.remove(os.path.join(tmp_dir, fn))
#         except Exception:
#             pass
#     try:
#         os.rmdir(tmp_dir)
#     except Exception:
#         pass

def process_one_paper(student_root: str, base_id: str, v1_only: bool=False, skip_ref: bool=False):
    yymm = to_yymm_id(base_id)
    paper_dir = os.path.join(student_root, yymm)
    ensure_dir(paper_dir)
    tex_root = os.path.join(paper_dir, "tex"); ensure_dir(tex_root)
    tmp_dir  = os.path.join(paper_dir, "_tmp"); ensure_dir(tmp_dir)

    versions = list_all_versions(base_id, v1_only=v1_only)
    any_ok = False
    for v in versions:
        arxiv_id_v = f"{base_id}v{v}"
        tgz_name   = f"{to_yymm_id(base_id)}v{v}.tar.gz"
        tgz_path   = os.path.join(tmp_dir, tgz_name)
        ok = try_download_source(arxiv_id_v, tmp_dir, tgz_name)
        if not ok:
            print(f"[WARN] No source for {arxiv_id_v}")
            continue
        out_dir = os.path.join(tex_root, f"{to_yymm_id(base_id)}v{v}")
        try:
            extract_tex_bib(tgz_path, out_dir)
            any_ok = True
        except ValueError:
            print(f"[WARN] Invalid tar for {arxiv_id_v}")

    # metadata & bib (không phụ thuộc có source hay không)
    meta = build_metadata(base_id, versions)
    write_json(os.path.join(paper_dir, "metadata.json"), asdict(meta))
    # try:
    #     bib = fetch_bibtex(base_id)
    #     with open(os.path.join(paper_dir, "references.bib"), "w", encoding="utf-8") as f:
    #         f.write(bib)
    # except Exception:
    #     pass

    # references.json (có throttle bên semantic_scholar.py)
    if not skip_ref:
        try:
            refs = get_references_with_arxiv_ids(base_id)
            refs_map = enrich_references_with_dates(refs)  # có throttle 1 req/s
            write_json(os.path.join(paper_dir, "references.json"), refs_map)
        except Exception:
            write_json(os.path.join(paper_dir, "references.json"), {})
    else:
        write_json(os.path.join(paper_dir, "references.json"), {})

    # cleanup
    for fn in os.listdir(tmp_dir):
        try: os.remove(os.path.join(tmp_dir, fn))
        except: pass
    try: os.rmdir(tmp_dir)
    except: pass

    return any_ok