import pandas as pd
from pathlib import Path

def transform_bronze():
    RAW_FOLDER_PATH = Path.cwd() / "data" / "bronze" / "reviews"
    OUTPUT_FOLDER_PATH = Path.cwd() / "data" / "silver" / "reviews" 
    if not Path.exists(OUTPUT_FOLDER_PATH): 
        Path.mkdir(OUTPUT_FOLDER_PATH)
        print(f"Created: {OUTPUT_FOLDER_PATH}")
    MAX_ROWS = 1_000_000

    

    bronze_year = [i.name for i in Path.iterdir(RAW_FOLDER_PATH)]
    silver_year = [i.name for i in Path.iterdir(OUTPUT_FOLDER_PATH)]
    # Years that are not transformed
    untransform_years = [year for year in bronze_year if year not in silver_year]
    print(f"Untransform years: {untransform_years}")


    
    for year in untransform_years:
        output_silver_year_paths = OUTPUT_FOLDER_PATH / year
        output_silver_year_paths.mkdir()
        print(f"Created: {output_silver_year_paths}")
        
        bronze_year_paths = RAW_FOLDER_PATH / year

        buffer = []
        part_file = 1
        line = 0
        for f in bronze_year_paths.glob("*.csv"):
            # xxxx_aa.csv -> xxxx
            game_id = f.resolve().name.split("_")[0]

            df = pd.read_csv(f)
            df.insert(0, "game_id", game_id)

            buffer.append(df)
            line += len(df)

            if line >= MAX_ROWS:
                file_path = output_silver_year_paths / f"part_{part_file}.csv"
                print(f"Write to: {file_path}")
                print(f"Line: {line}")
                print(f"Buffer: {len(buffer)}")

                merged = pd.concat(buffer)
                merged.to_csv(file_path, index=False)

                buffer = []
                line = 0
                part_file += 1

        if buffer:
            file_path = output_silver_year_paths / f"part_{part_file}.csv"
            print(f"Write to: {file_path}")
            print(f"Write to: {file_path}")
            print(f"Line: {line}")

            merged = pd.concat(buffer)
            merged.to_csv(file_path, index=False)

    print("TRANSFORMING DONE")
