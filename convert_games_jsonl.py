"""
convert_games_jsonl.py — Chuyển games.json (1 object lớn) sang JSON Lines
Chạy trong spark-notebook:
    docker exec spark-notebook python /home/jovyan/work/convert_games_jsonl.py
"""
import json
import os

# spark-notebook container mount project dir tại /home/jovyan/work
# và namenode mount ./data tại /opt/data
# Nhưng spark-notebook cũng mount ./ tại /home/jovyan/work
# Vậy file local sẽ là /home/jovyan/work/data/silver/metadata/games.json
LOCAL_INPUT = "/home/jovyan/work/data/silver/metadata/games.json"
LOCAL_OUTPUT = "/home/jovyan/work/data/silver/metadata/games_lines.jsonl"


print(f"Reading {LOCAL_INPUT}...")
with open(LOCAL_INPUT, "r", encoding="utf-8") as f:
    data = json.load(f)

count = 0
with open(LOCAL_OUTPUT, "w", encoding="utf-8") as f:
    for app_id, info in data.items():
        info["app_id"] = app_id
        f.write(json.dumps(info, ensure_ascii=False) + "\n")
        count += 1

print(f"Converted {count} games → {LOCAL_OUTPUT}")
print("Done!")
