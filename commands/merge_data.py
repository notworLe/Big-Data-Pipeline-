import pandas as pd
from pathlib import Path


def merge():
    GAMES_PATH = Path.cwd() / "data" / "metadata" / "games.json"
    RAW_FOLDER_PATH = Path.cwd() / "data" / "reviews" / "raw"
    OUTPUT_FOLDER_PATH = Path.cwd() / "data" / "reviews"  / "merged"
    MAX_ROWS = 1_000_000

    buffer = []
    part_file = 1
    line = 0

    for f in Path.iterdir(RAW_FOLDER_PATH):
        # xxxx_aa.csv -> xxxx
        game_id = f.resolve().name.split("_")[0]

        df = pd.read_csv(f)
        df.insert(0, "game_id", game_id)

        buffer.append(df)
        line += len(df)

        if line >= MAX_ROWS:
            file_path = OUTPUT_FOLDER_PATH / f"merged_part_{part_file}.csv"
            print(f"Write to: {file_path}")
            print(f"Line: {line}")
            print(f"Buffer: {len(buffer)}")

            merged = pd.concat(buffer)
            merged.to_csv(file_path, index=False)

            buffer = []
            line = 0
            part_file += 1

    if buffer:
        file_path = OUTPUT_FOLDER_PATH / f"merged_part_{part_file}.csv"
        print(f"Write to: {file_path}")
        print(f"Write to: {file_path}")
        print(f"Line: {line}")

        merged = pd.concat(buffer)
        merged.to_csv(file_path, index=False)