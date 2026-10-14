# Bài thực hành Hadoop: Doanh thu theo nhóm sản phẩm (HDFS + MapReduce)

> Môn Dữ liệu lớn – Chapter 2: Hadoop Ecosystem
> Họ tên / MSSV: `<điền vào>`

## 1. Bài toán
Sàn thương mại điện tử có file đơn hàng tháng 09/2026 (`data/orders_2026_09.csv`, **1.500 giao dịch**, 11 cột).
**Câu hỏi:** nhóm sản phẩm (`category`) nào có tổng doanh thu cao nhất, **chỉ tính giao dịch `status = SUCCESS`**?

## 2. Cấu trúc thư mục
```
data/orders_2026_09.csv          dữ liệu đầu vào
src/mapper_revenue.py            Mapper
src/reducer_revenue.py           Reducer
scripts/run_local.sh             chạy thử pipeline cat | mapper | sort | reducer
scripts/run_hadoop_hdfs.sh       chạy Hadoop Streaming thật trên HDFS (LabEx)
scripts/verify_pandas.py         đối chiếu kết quả với Pandas
Hadoop_Ecommerce_Lab.ipynb       notebook Google Colab (Hadoop Streaming local mode)
output/part-00000                kết quả doanh thu theo category
output/top3.txt                  Top 3
screenshots/                     (bạn thêm) ảnh chụp minh chứng
```

## 3. Cách giải quyết
Luồng dữ liệu: `HDFS input → Mapper → Shuffle/Sort → Reducer → HDFS output`

1. **Dữ liệu vào HDFS** – tạo `/user/student/ecommerce/input` và `output`, `hadoop fs -put` file CSV lên, kiểm tra bằng `hadoop fs -ls` và `hadoop fs -head`. HDFS chia file thành block, lưu phân tán và nhân bản (replication) để chịu lỗi.
2. **Mapper** (`mapper_revenue.py`) – đọc từng dòng CSV từ `stdin` bằng `csv.DictReader`. Nếu `status == "SUCCESS"` thì in cặp **key–value = `category \t amount`**, ví dụ `Electronics	12741238`. Các giao dịch `FAILED`/`RETURNED` bị loại ngay ở bước này (1.240/1.500 dòng được phát sinh).
3. **Shuffle/Sort** – do Hadoop tự làm: gom và sắp xếp các cặp theo key, nên mọi dòng cùng `category` nằm liền nhau khi vào Reducer. (Khi chạy thử local phải tự gọi `sort`.)
4. **Reducer** (`reducer_revenue.py`) – duyệt các dòng đã sắp xếp, cộng dồn `amount` cho đến khi key đổi thì in `category \t tổng`. Kết quả ghi vào `part-00000` trong thư mục output.
5. **Top 3** – `sort -k2,2nr part-00000 | head -3`.
6. **Kiểm chứng** – `scripts/verify_pandas.py` tính lại bằng Pandas (`groupby('category').amount.sum()` trên các dòng SUCCESS): **khớp 8/8 category**.
7. **YARN** – khi chạy job, YARN (ResourceManager/NodeManager) cấp container cho các task map và reduce; có thể xem bằng `yarn application -list`. Ở local mode trên Colab không có YARN thật.

### Cách chạy
```bash
# Thử nhanh (không cần Hadoop)
bash scripts/run_local.sh && python3 scripts/verify_pandas.py

# Hadoop thật trên HDFS (LabEx hoặc Colab pseudo-distributed)
bash scripts/run_hadoop_hdfs.sh

# Colab: mở Hadoop_Ecommerce_Lab.ipynb, upload 3 file vào /content rồi Run all
```

## 4. Kết quả (đầy đủ 8 category, đơn vị: đồng)
| Category | Doanh thu | Tỷ trọng |
|---|---:|---:|
| Electronics | 3.703.769.311 | 76,94% |
| Home | 410.315.065 | 8,52% |
| Fashion | 218.597.763 | 4,54% |
| Sport | 166.194.354 | 3,45% |
| Beauty | 111.032.003 | 2,31% |
| Toy | 83.881.779 | 1,74% |
| Grocery | 80.983.147 | 1,68% |
| Book | 39.306.022 | 0,82% |
| **Tổng** | **4.814.079.444** | 100% |

### Báo cáo Top 3
| Hạng | Category | Doanh thu | Nhận xét |
|---:|---|---:|---|
| 1 | Electronics | 3.703.769.311 | Cao nhất, chiếm ~77% tổng doanh thu |
| 2 | Home | 410.315.065 | Đứng thứ hai, bằng ~11% doanh thu Electronics |
| 3 | Fashion | 218.597.763 | Ổn định, chiếm ~4,5% |

### Nhận xét kinh doanh
- **Electronics** tạo gần ba phần tư doanh thu (3,70 tỷ / 4,81 tỷ) dù chỉ có 296/1.240 đơn thành công (~24% số đơn) – giá trị mỗi đơn rất cao.
- Doanh thu **phụ thuộc mạnh vào một nhóm**; biến động của Electronics (khuyến mại, tồn kho, hàng lỗi/hoàn) ảnh hưởng trực tiếp đến toàn sàn, cần ưu tiên quản trị tồn kho và giao hàng cho nhóm này.
- **Home** (410 triệu) và **Fashion** (219 triệu) là hai nhóm phụ có thể đẩy bằng combo/khuyến mại.
- Grocery và Book có nhiều đơn (229 và 101) nhưng giá trị thấp, tổng doanh thu nhỏ – hợp làm nhóm kéo lưu lượng hơn là nhóm tạo lợi nhuận.
- Chỉ tính SUCCESS: 1.240 đơn; 142 FAILED và 118 RETURNED không được tính (đúng yêu cầu đề).

### Hạn chế của xử lý batch
Hadoop MapReduce xử lý theo lô nên có **độ trễ cao** (khởi tạo job, ghi đĩa giữa các bước): báo cáo chỉ có sau khi job chạy xong trên toàn bộ dữ liệu, không phù hợp nhu cầu theo dõi doanh thu thời gian thực (cần Spark Streaming/Flink...). Với file nhỏ như 1.500 dòng, chi phí khởi động job còn lớn hơn thời gian tính toán.

## 5. Lưu ý trung thực về cách tạo kết quả
Kết quả trong `output/` được tạo bằng **pipeline tương đương Hadoop Streaming** (`cat | mapper | sort | reducer`) và đã đối chiếu với Pandas, vì môi trường soạn bài này **không cài/tải được Hadoop**. Đề yêu cầu kết quả chính thức từ Hadoop Streaming, nên bạn cần chạy lại `scripts/run_hadoop_hdfs.sh` (LabEx) hoặc notebook Colab, rồi **chụp màn hình** và bỏ vào `screenshots/`:
- [ ] `hadoop version`
- [ ] `hadoop fs -ls` và `hadoop fs -head` trên input
- [ ] lệnh `hadoop jar ... hadoop-streaming ...` chạy thành công
- [ ] `hadoop fs -ls` thư mục output và `hadoop fs -cat .../part-*`
- [ ] Top 3 (và ResourceManager/`yarn application -list` nếu có)

Số liệu Hadoop thật sẽ trùng với bảng trên (thứ tự dòng có thể khác).

## 6. Mở rộng (đã làm)
- Đối chiếu MapReduce với Pandas – `scripts/verify_pandas.py` ✔.
