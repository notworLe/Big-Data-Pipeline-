from kafka import KafkaProducer
import json
import time
import csv
import os
import glob

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

DATA_DIR = r"C:\Users\MANH\hdfs-infrastructure\data\sample2"

csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
print(f"Found {len(csv_files)} CSV files")

count = 0
for file_path in csv_files:
    game_name = os.path.basename(file_path).replace(".csv", "")
    try:
        with open(file_path, encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            for row in reader:
                message = {
                    'app_id': row.get('app_id', ''),
                    'game_name': game_name,
                    'review': row.get('review', '')[:500],
                    'voted_up': row.get('voted_up', ''),
                    'timestamp': row.get('timestamp_created', '')
                }
                producer.send('steam-reviews', message)
                count += 1
                if count % 100 == 0:
                    print(f"Sent {count} reviews | Latest game: {game_name}")
                time.sleep(0.1)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        continue

producer.flush()
print(f"Done! Total sent: {count} reviews")