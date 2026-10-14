#!/usr/bin/env bash
# Chạy thử logic MapReduce bằng pipeline Unix (tương đương Hadoop Streaming):
#   cat input | mapper | sort (shuffle/sort) | reducer
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p output
cat data/orders_2026_09.csv | python3 src/mapper_revenue.py | sort | python3 src/reducer_revenue.py > output/part-00000
echo "== Doanh thu theo category =="; cat output/part-00000
echo "== Top 3 =="; sort -k2,2nr output/part-00000 | head -3 | tee output/top3.txt
