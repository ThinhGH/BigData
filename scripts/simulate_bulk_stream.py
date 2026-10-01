import time
import json
import pandas as pd
from kafka import KafkaProducer

# Cấu hình Kafka (Lưu ý: Chạy từ Windows host nên gọi cổng 29092 của Kafka)
KAFKA_SERVER = 'localhost:29092'
KAFKA_TOPIC = 'ratings'

# Đường dẫn file CSV bạn muốn "xả" vào hệ thống streaming
# (Bạn có thể đổi tên file này thành file data mới mà bạn vừa tải về)
CSV_FILE_PATH = r'E:\BigData\data\raw\ml-latest-small\ratings.csv'

def main():
    print(f"Đang kết nối tới Kafka tại {KAFKA_SERVER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"❌ Lỗi kết nối Kafka: {e}. Vui lòng kiểm tra xem Docker đã chạy chưa.")
        return

    print(f"Đang đọc dữ liệu từ {CSV_FILE_PATH}...")
    try:
        # Tải dữ liệu bằng Pandas
        df = pd.read_csv(CSV_FILE_PATH)
    except Exception as e:
        print(f"❌ Không tìm thấy hoặc không đọc được file CSV: {e}")
        return

    # Sắp xếp dữ liệu theo thời gian (timestamp) để mô phỏng y hệt thực tế
    df = df.sort_values(by='timestamp')

    print(f"✅ Bắt đầu xả {len(df)} dòng dữ liệu vào Kafka...")
    
    count = 0
    start_time = time.time()
    
    # Duyệt qua từng dòng và gửi vào Kafka
    for index, row in df.iterrows():
        # Đóng gói dữ liệu thành JSON theo chuẩn hệ thống
        payload = {
            "userId": int(row['userId']),
            "movieId": int(row['movieId']),
            "rating": float(row['rating']),
            "timestamp": int(row['timestamp'])
        }
        
        producer.send(KAFKA_TOPIC, payload)
        count += 1
        
        # Cứ gửi được 10,000 tin nhắn thì in ra tiến độ một lần
        if count % 10000 == 0:
            print(f"⏳ Đã gửi {count}/{len(df)} ratings...")
            
        # Tùy chọn: Thêm delay nhỏ nếu bạn không muốn gửi quá nhanh (gây nghẽn)
        # time.sleep(0.001)

    # Đợi Kafka gửi nốt các tin nhắn còn tồn đọng
    producer.flush()
    elapsed = time.time() - start_time
    
    print("==================================================")
    print(f"🎯 HOÀN TẤT! Đã đẩy thành công {count} ratings vào Kafka topic '{KAFKA_TOPIC}'.")
    print(f"Thời gian xả dữ liệu: {elapsed:.2f} giây.")
    print("Bây giờ Spark Structured Streaming sẽ tự động hút dữ liệu này từ Kafka và ghi vào Delta Lake!")
    print("==================================================")

if __name__ == "__main__":
    main()
