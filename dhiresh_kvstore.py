#!/usr/bin/env python3
"""
Project: Simple Persistent Key–Value Store (Project 1)
Language: Python 3.x

Commands:
  SET <key> <value>
  GET <key>
  EXIT

- Append-only persistence to data.db
- Replay on startup
- In-memory index uses a list (no dict/map)
"""

import os
import sys

DATA_FILE = "data.db"

class KeyValueStore:
    def __init__(self):
        self.data = []  # list of [key, value]
        self.load_data()

    def load_data(self):
        """Read data from file on startup."""
        if not os.path.exists(DATA_FILE):
            open(DATA_FILE, "a").close()
        with open(DATA_FILE, "r") as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) == 3 and parts[0] == "SET":
                    key, value = parts[1], parts[2]
                    self.set_in_memory(key, value)

    def set_in_memory(self, key, value):
        """Update or add a key-value pair in memory."""
        for pair in self.data:
            if pair[0] == key:
                pair[1] = value
                return
        self.data.append([key, value])

    def get_from_memory(self, key):
        """Return value for key if exists."""
        for pair in reversed(self.data):  # last write wins
            if pair[0] == key:
                return pair[1]
        return None

    def set(self, key, value):
        """Set value in memory and append to file."""
        self.set_in_memory(key, value)
        with open(DATA_FILE, "a") as f:
            f.write(f"SET {key} {value}\n")
            f.flush()
            os.fsync(f.fileno())

    def get(self, key):
        """Return the value if present, else None (print empty for missing)."""
        return self.get_from_memory(key)


def main():
    store = KeyValueStore()

    while True:
        try:
            command = input().strip()
        except EOFError:
            break

        if not command:
            continue

        parts = command.split()
        cmd = parts[0].upper()

        if cmd == "SET" and len(parts) == 3:
            key, value = parts[1], parts[2]
            store.set(key, value)
            print("OK")
            sys.stdout.flush()
        elif cmd == "GET" and len(parts) == 2:
            key = parts[1]
            val = store.get(key)
            print("" if val is None else val)   # empty line for missing key
            sys.stdout.flush()
        elif cmd == "EXIT":
            break
        else:
            print("ERR: Invalid command")
            sys.stdout.flush()

if __name__ == "__main__":
    main()
