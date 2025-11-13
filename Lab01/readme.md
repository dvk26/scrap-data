

1. Cài đặt

pip install -r src/requirements.txt
Dependencies: arxiv, requests, beautifulsoup4, python-dateutil, tqdm, semanticscholar

2. Cách sử dụng

Main command:

python -m src.main ...

A. Crawl bằng danh sách ID trực tiếp

Ví dụ crawl các bài:
1706.03762
2310.12345
Command:
python -m src.main --student-id 22127227 --ids 1706.03762 2310.12345 --max-workers 6
Output sẽ nằm trong:
./22127227/

B. Crawl theo MONTH RANGE
Ví dụ:
month: 2024-04
range: 198 → 5197
Command:

python -m scrap.main --student-id 22127227 --month 2024-04 --start 198 --end 5197 --max-workers 8

E. Chọn custom output directory
python -m scrap.main --student-id 22127227 --out E:/DS/Lab01_Output --month 2024-04 --start 198 --end 5197

F. Thêm delay giữa từng paper (hạn chế 429)
python -m scrap.main --student-id 22127227 --month 2024-04 --start 198 --end 500 --sleep-between-papers 0.8

* Max workers có thể thay đổi tùy theo custom nhưng ưu tiên trong khoảng từ 6 -> 8 để tránh lỗi 429 từ phía server.

Cây thư mục cho mỗi paper:

22127227/
│
└── 2404-00198/
    │
    ├── tex/
    │   ├── 2404-00198v1/
    │   │    *.tex / *.bib ...
    │   ├── 2404-00198v2/
    │   │    ...
    │
    ├── metadata.json
    └── references.json

