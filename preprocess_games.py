import json
import os

def preprocess():
    input_path = "data/metadata/games.json"
    output_path = "data/metadata/games_cleaned.json"
    
    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found.")
        return
        
    print("Reading games.json...")
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    print(f"Loaded {len(data)} games. Writing to games_cleaned.json as JSON Lines...")
    with open(output_path, 'w', encoding='utf-8') as f:
        for app_id, meta in data.items():
            # Add app_id to the record
            meta['app_id'] = app_id
            f.write(json.dumps(meta, ensure_ascii=False) + '\n')
            
    print("Preprocess completed successfully!")

if __name__ == "__main__":
    preprocess()
