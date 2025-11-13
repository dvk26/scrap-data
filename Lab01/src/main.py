import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
from typing import List, Tuple
from .utils import ensure_dir
from .pipeline import process_one_paper
from .range_builder import expand_many


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--student-id", required=True, help="e.g., 22127227")
    # direct IDs
    ap.add_argument("--ids", nargs="*", default=[], help="List of arXiv IDs, e.g., 1706.03762 2310.12345")
    ap.add_argument("--out", help="Custom output directory (default: ./<student-id>)")   # ✅ NEW

    # month range blocks (can repeat)
    ap.add_argument("--month", action="append", help="YYYY-MM, e.g., 2024-04")
    ap.add_argument("--start", action="append", type=int, help="Start number in month, e.g., 198")
    ap.add_argument("--end",   action="append", type=int, help="End number in month, e.g., 5197")
    # performance knobs
    ap.add_argument("--max-workers", type=int, default=6, help="Parallel workers for source/tex")
    ap.add_argument("--skip-ref", action="store_true", help="Skip Semantic Scholar references")
    ap.add_argument("--v1-only", action="store_true", help="Download only v1")
    ap.add_argument("--sleep-between-papers", type=float, default=0.0, help="Sleep between papers (seconds)")
    return ap.parse_args()


def collect_ids(args) -> List[str]:
    ids: List[str] = []
    ids.extend(args.ids)
    if args.month or args.start or args.end:
        if not (args.month and args.start and args.end):
            raise SystemExit("When using --month/--start/--end, all three must be provided (and repeated equally).")
        if not (len(args.month) == len(args.start) == len(args.end)):
            raise SystemExit("Mismatched counts of --month/--start/--end.")
        month_ranges: List[Tuple[str, int, int]] = list(zip(args.month, args.start, args.end))
        ids.extend(expand_many(month_ranges))
    if not ids:
        raise SystemExit("No IDs provided. Use --ids ... or --month ... --start ... --end ...")
    ids = [i.split("v")[0] for i in ids]  # normalize
    seen, ordered = set(), []
    for i in ids:
        if i not in seen:
            ordered.append(i); seen.add(i)
    return ordered


def main():
    args = parse_args()
    # ✅ root chọn theo --out nếu có, ngược lại fallback về student-id
    root = os.path.abspath(args.out if args.out else args.student_id)
    ensure_dir(root)
    print(f"Output directory: {root}")  # ✅ log ra màn hình

    ids = collect_ids(args)
    total = len(ids)
    start_time = time.time()
    start_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))
    print(f"[{start_str}] Start crawling {total} papers with {args.max_workers} worker(s).")

    def run_one(aid: str, idx: int):
        t0 = time.time()
        process_one_paper(root, aid, v1_only=args.v1_only, skip_ref=args.skip_ref)
        if args.sleep_between_papers > 0:
            time.sleep(args.sleep_between_papers)
        per_paper = time.time() - t0
        since_start = time.time() - start_time
        # return data for logging in the main thread
        return {
            "idx": idx,
            "aid": aid,
            "per_paper": per_paper,
            "since_start": since_start,
        }

    # submit all jobs and keep their original index
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futures = {ex.submit(run_one, aid, i + 1): (i + 1, aid) for i, aid in enumerate(ids)}
        completed = 0
        for fut in as_completed(futures):
            info = fut.result()
            completed += 1
            # English log line for each completed paper
            now_str = time.strftime("%H:%M:%S")
            print(
                f"[{now_str}] Paper {info['idx']}/{total} ({info['aid']}) completed — "
                f"duration {info['per_paper']:.2f}s, elapsed {info['since_start']:.2f}s since start."
            )

    total_time = time.time() - start_time
    print(f"All {total} papers finished in {total_time:.2f}s total.")
    print(f"Run to package: zip -r {args.student_id}.zip {args.student_id}")


if __name__ == "__main__":
    main()
