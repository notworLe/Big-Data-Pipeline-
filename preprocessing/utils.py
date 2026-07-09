from pathlib import Path

BASE_PATH = Path(__file__).parent.parent
BRONZE_PATH = BASE_PATH / "data" / "bronze"
SILVER_PATH = BASE_PATH / "data" / "silver"

LAYER = ['bronze', 'silver', 'gold']

def get_year_from_reviews(layer='bronze'):
    if layer not in LAYER:
        raise ValueError(f"Layer must be one of {LAYER}")
    
    if layer == 'bronze':
        source_path = BRONZE_PATH
    elif layer == 'silver':
        source_path = SILVER_PATH
    else:
        raise ValueError(f"Layer must be one of {LAYER}")
    
    return [year for year in Path.iterdir(Path(source_path) / "reviews")]


if __name__ == "__main__":
    print(get_year_from_reviews())