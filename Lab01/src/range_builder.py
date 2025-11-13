from typing import List, Tuple

def yymm_from_month(month_yyyy_mm: str) -> str:
    # "2024-04" -> "2404"
    yyyy, mm = month_yyyy_mm.split("-")
    return f"{int(yyyy)%100:02d}{int(mm):02d}"

def make_ids_for_range(month_yyyy_mm: str, start: int, end: int) -> List[str]:
    """
    Sinh danh sách IDs: 2404.00198 .. 2404.05197 (bao gồm cả start và end)
    start/end là số thứ tự (1..99999). Tự pad 5 chữ số.
    """
    if start < 1 or end < start or end > 99999:
        raise ValueError("Invalid range: ensure 1 <= start <= end <= 99999")
    yymm = yymm_from_month(month_yyyy_mm)
    return [f"{yymm}.{i:05d}" for i in range(start, end + 1)]

def expand_many(month_ranges: List[Tuple[str, int, int]]) -> List[str]:
    ids: List[str] = []
    for m, s, e in month_ranges:
        ids.extend(make_ids_for_range(m, s, e))
    return ids
