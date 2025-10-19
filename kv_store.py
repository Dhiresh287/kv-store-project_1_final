import os

DATA_FILE = "data.db"

# In-memory store: list of (key, value) tuples
kv_store = []

def load_data():
    if not os.path.exists(DATA_FILE):
        return
    with open(DATA_FILE, "r") as f:
        for line in f:
            parts = line.strip().split(" ", 2)
            if len(parts) == 3 and parts[0] == "SET":
                set_in_memory(parts[1], parts[2])

def append_to_log(key, value):
    with open(DATA_FILE, "a") as f:
        f.write(f"SET {key} {value}\n")

def set_in_memory(key, value):
    for i, (k, v) in enumerate(kv_store):
        if k == key:
            kv_store[i] = (key, value)
            return
    kv_store.append((key, value))

def get_from_memory(key):
    for k, v in reversed(kv_store):
        if k == key:
            return v
    return None

def main():
    load_data()
    while True:
        try:
            command = input().strip()
            if command.upper() == "EXIT":
                break
            parts = command.split(" ", 2)
            if parts[0] == "SET" and len(parts) == 3:
                key, value = parts[1], parts[2]
                append_to_log(key, value)
                set_in_memory(key, value)
            elif parts[0] == "GET" and len(parts) == 2:
                value = get_from_memory(parts[1])
                print(value if value else "key not found")
            else:
                print("invalid command")
        except EOFError:
            break

if __name__ == "__main__":
    main()
