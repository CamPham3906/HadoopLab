"""Đối chiếu kết quả MapReduce với Pandas (phần mở rộng #5)."""
import sys
import pandas as pd

df = pd.read_csv("data/orders_2026_09.csv")
expected = df[df.status == "SUCCESS"].groupby("category").amount.sum()
got = {}
for line in open(sys.argv[1] if len(sys.argv) > 1 else "output/part-00000"):
    k, v = line.rstrip("\n").split("\t")
    got[k] = int(v)
assert got == expected.to_dict(), "KHÔNG KHỚP"
print("OK: MapReduce == Pandas cho", len(got), "category")
