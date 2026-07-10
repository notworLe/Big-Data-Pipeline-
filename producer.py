import csv, json, time
from kafka import KafkaProducer
import os

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = "steam-reviews"
# Sửa lại path để dùng file silver đã gộp (part_1)
CSV_PATH = "/home/jovyan/work/data/silver/reviews/2020/part_1.csv"

def send_reviews(target_game_id=None, rate=10, duration=None):
    """
    target_game_id=None → baseline mode, trộn đều mọi game
    target_game_id="123" → burst mode, dồn vào 1 game để demo alert
    rate → số message/giây
    """
    if not os.path.exists(CSV_PATH):
        print(f"Error: File {CSV_PATH} not found.")
        return
        
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        count = 0
        start = time.time()
        for row in reader:
            # Bỏ qua các dòng lỗi (nếu có)
            if not row.get("game_id") or not row.get("recommend"):
                continue
                
            if target_game_id and row["game_id"] != target_game_id:
                continue
                
            producer.send(TOPIC, {
                "game_id": row["game_id"],
                "recommend": row["recommend"],  # cột trong dataset là recommend (Recommended/Not Recommended)
                "review_text": row.get("review", "")[:200],  # cắt bớt cho nhẹ
                "event_time": time.time() # time.time() là float (giây) -> DoubleType
            })
            count += 1
            if count % rate == 0:
                time.sleep(1)
            if duration and (time.time() - start) > duration:
                break
    producer.flush()
    print(f"Sent {count} messages.")

if __name__ == "__main__":
    # Mode 1 — baseline: chạy nền liên tục (comment lại để ko tự chạy)
    # send_reviews(rate=10)
    print("Run inside notebook container:")
    print("python -c \"from producer import send_reviews; send_reviews(rate=10)\"")
