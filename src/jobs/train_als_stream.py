#!/usr/bin/env python
"""Job: Cập nhật model ALS từ dữ liệu Delta Lake (streaming đã ghi).

- Đọc Delta Lake (hoặc Parquet gốc nếu Delta chưa tồn tại).
- Tính train/val/test bằng hàm `add_split_column`.
- Fit ALS, lưu model vào `config.MODEL_DIR`.
"""

import csv as csv_module
from pathlib import Path

from src import config
from src.common.split import add_split_column, count_excluded_users
from src.session import get_spark
from src.jobs.train_als import build_als, evaluate_rmse, grid_search


def main() -> None:
    spark = get_spark("train_als_stream")

    delta_path = Path(str(config.DELTA_LAKE_PATH))

    # Ưu tiên đọc Delta Lake (dữ liệu streaming đã ghi).
    # Nếu Delta chưa có, fallback về Parquet gốc (batch pipeline).
    if delta_path.exists() and any(delta_path.iterdir()):
        print(f"Đọc dữ liệu từ Delta Lake: {config.DELTA_LAKE_PATH}")
        ratings = spark.read.format("delta").load(str(config.DELTA_LAKE_PATH))
    elif Path(str(config.RATINGS_PARQUET)).exists():
        print(f"Delta Lake chưa tồn tại, fallback về Parquet: {config.RATINGS_PARQUET}")
        ratings = spark.read.parquet(str(config.RATINGS_PARQUET))
    else:
        print("Không tìm thấy dữ liệu (Delta Lake hoặc Parquet). Hãy chạy pipeline trước.")
        spark.stop()
        return

    excluded = count_excluded_users(ratings)
    print(f"Excluded users (cold-start): {excluded}")

    split = add_split_column(ratings)
    train = split.filter("split = 'train'").select("userId", "movieId", "rating")
    validation = split.filter("split = 'val'").select("userId", "movieId", "rating")

    # Grid search (hoặc fixed param nếu env vars được đặt)
    results = grid_search(train, validation)

    # Lưu best model
    best = results[0]
    print(f"Tổ hợp tốt nhất: rank={best['rank']} regParam={best['regParam']} RMSE={best['rmse']}")

    refit = train.union(validation)
    final_model = build_als(best["rank"], best["regParam"]).fit(refit)
    final_model.write().overwrite().save(str(config.MODEL_DIR))

    # Ghi best_params
    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    best_path = config.RESULTS_DIR / "best_params.csv"
    with open(best_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv_module.DictWriter(fh, fieldnames=list(best) + ["excluded_users"])
        writer.writeheader()
        writer.writerow({**best, "excluded_users": excluded})

    print(f"Model saved to {config.MODEL_DIR}")

    # Chạy luôn job export để sinh lại file recs.sqlite
    # Nhờ mtime của file này thay đổi, FastAPI sẽ tự động hot-reload Model
    print("Đang cập nhật lại CSDL SQLite (export_recs)...")
    try:
        from src.jobs import export_recs
        export_recs.main()
    except Exception as e:
        print(f"Lỗi khi export SQLite: {e}")

    spark.stop()


if __name__ == "__main__":
    main()