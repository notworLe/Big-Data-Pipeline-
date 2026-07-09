from pathlib import Path


current = Path.cwd() / "data" / "silver" / "reviews"
data = "a" /  current / "data" 

print(current)
print(current / data)