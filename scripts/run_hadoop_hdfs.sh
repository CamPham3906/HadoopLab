#!/usr/bin/env bash
# Chạy job Hadoop Streaming THẬT trên HDFS (LabEx / Colab pseudo-distributed).
# Chạy từ thư mục chứa data/ và src/. Cần: hadoop, HADOOP_HOME đã đặt.
set -euo pipefail
BASE=/user/student/ecommerce
chmod +x src/mapper_revenue.py src/reducer_revenue.py

hadoop version
hadoop fs -mkdir -p $BASE/input $BASE/output
hadoop fs -put -f data/orders_2026_09.csv $BASE/input/
hadoop fs -ls $BASE/input
hadoop fs -head $BASE/input/orders_2026_09.csv

hadoop fs -rm -r -f $BASE/output/revenue_by_category
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
  -files src/mapper_revenue.py,src/reducer_revenue.py \
  -mapper "python3 mapper_revenue.py" \
  -reducer "python3 reducer_revenue.py" \
  -input  $BASE/input/orders_2026_09.csv \
  -output $BASE/output/revenue_by_category

hadoop fs -ls  $BASE/output/revenue_by_category
hadoop fs -cat $BASE/output/revenue_by_category/part-*
mkdir -p output
hadoop fs -get -f $BASE/output/revenue_by_category/part-* output/result_revenue_by_category.txt
sort -k2,2nr output/result_revenue_by_category.txt | head -3
