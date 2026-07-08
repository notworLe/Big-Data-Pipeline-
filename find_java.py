import os

def find_java():
    search_dirs = [
        r"C:\Program Files",
        r"C:\Program Files (x86)",
        os.path.expanduser("~")
    ]
    
    found_javas = []
    
    for base_dir in search_dirs:
        if not os.path.exists(base_dir):
            continue
        print(f"Searching in {base_dir}...")
        for root, dirs, files in os.walk(base_dir):
            # Skip directories to speed up search
            if any(p in root.lower() for p in ["node_modules", ".git", "appdata\\local\\temp"]):
                continue
            if "java.exe" in files:
                path = os.path.join(root, "java.exe")
                found_javas.append(path)
                print(f"Found: {path}")
                # Don't recurse deeper if java.exe is found in this branch
                dirs.clear()
                
    print("\nSearch results:")
    for path in found_javas:
        print(path)

if __name__ == "__main__":
    find_java()
