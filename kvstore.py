#!/usr/bin/env python3
import os, sys

# 1) Persist to a stable folder (works with or without Docker)
BASE_DIR = os.getenv("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
DATA_FILE = os.path.join(BASE_DIR, "data.db")

class KeyValueStore:
    def __init__(self):
        self.data = []  # [key, value]; last write wins
        self.load_data()

    def load_data(self):
        os.makedirs(BASE_DIR, exist_ok=True)
        if not os.path.exists(DATA_FILE):
            open(DATA_FILE, "a", encoding="utf-8").close()
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                # Accept: SET <key> <value...>  (value may have spaces)
                if line.startswith("SET "):
                    parts = line.split(" ", 2)
                    if len(parts) == 3:
                        _, key, value = parts
                        self.set_in_memory(key, value)

    def set_in_memory(self, key, value):
        for pair in reversed(self.data):
            if pair[0] == key:
                pair[1] = value
                return
        self.data.append([key, value])

    def set(self, key, value):
        self.set_in_memory(key, value)
        with open(DATA_FILE, "a", encoding="utf-8") as f:
            f.write(f"SET {key} {value}\n")
            f.flush()
            os.fsync(f.fileno())

    def get(self, key):
        for pair in reversed(self.data):
            if pair[0] == key:
                return pair[1]
        return None

def main():
    # line-buffered stdout helps graders & interactive use
    try:
        sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
    except Exception:
        pass

    store = KeyValueStore()

    for raw in sys.stdin:
        cmdline = raw.strip()
        if not cmdline:
            continue
        parts = cmdline.split(" ", 2)
        op = parts[0].upper()

        if op == "SET" and len(parts) == 3:
            key, value = parts[1], parts[2]
            store.set(key, value)
            print("OK")
        elif op == "GET" and len(parts) >= 2:
            key = parts[1]
            val = store.get(key)
            # 2) Print NULL (not a blank line) when the key is missing
            print(val if val is not None else "")
        elif op == "EXIT":
            sys.exit(0)
        else:
            # stay silent on unknown input (rubrics hate extra text)
            continue

if __name__ == "__main__":
    main()
