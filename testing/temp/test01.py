import pandas as pd
from pathlib import Path

GAME_PATH = Path.cwd() / "data" / "silver" / "metadata" / "games.json"

df = pd.read_json(GAME_PATH)
print(df.head())