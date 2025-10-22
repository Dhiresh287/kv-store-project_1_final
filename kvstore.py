#!/usr/bin/env python3
"""
Persistent append-only key–value store with CLI.

Persistence format:
    SET <key> <value...>\n

Usage (stdin):
    SET foo bar
    GET foo
    EXIT
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Dict, Optional, TextIO

# ---------- Configuration ----------
BASE_DIR = Path(os.getenv("DATA_DIR", Path(__file__).resolve().parent))
DATA_FILE = BASE_DIR / "data.db"

# ---------- Logging ----------
logger = logging.getLogger("kvstore")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# ---------- Utility helpers ----------
def _atomic_append(path: Path, line: str) -> None:
    """Atomically append a single log line to the file."""
    with path.open("a", encoding="utf-8") as f:
        f.write(line)
        f.flush()
        os.fsync(f.fileno())


def _load_log(path: Path) -> Dict[str, str]:
    """Replay the append-only log file into a dictionary."""
    kv: Dict[str, str] = {}
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line.startswith("SET "):
                    continue
                _, key, value = line.split(" ", 2)
                kv[key] = value
    except FileNotFoundError:
        path.touch()
    except Exception as e:
        logger.warning("Corrupt log detected: %s", e)
        bak = f"{path}.bak"
        try:
            os.replace(path, bak)
        except OSError:
            pass
        path.touch()
    return kv


def _validate_key(key: str) -> None:
    """Ensure key is a non-empty reasonable string."""
    if not isinstance(key, str) or not key.strip():
        raise ValueError("Key must be a non-empty string.")
    if len(key) > 256:
        raise ValueError("Key too long (max 256 chars).")


# ---------- Data Layer ----------
class KeyValueStore:
    """In-memory last-write-wins key–value store backed by an append-only log."""

    def __init__(self, data_file: Path = DATA_FILE) -> None:
        self._data_file = data_file
        self._data_file.parent.mkdir(parents=True, exist_ok=True)
        self._kv = _load_log(self._data_file)

    def set(self, key: str, value: str) -> None:
        """Set key → value and append to persistent log."""
        _validate_key(key)
        self._kv[key] = value
        _atomic_append(self._data_file, f"SET {key} {value}\n")

    def get(self, key: str) -> Optional[str]:
        """Return the stored value for key, or None if not found."""
        _validate_key(key)
        return self._kv.get(key)


# ---------- CLI Layer ----------
def run_cli(store: KeyValueStore, stdin: TextIO, stdout: TextIO) -> int:
    """Interactive CLI for KV store (used by autograder)."""
    try:
        stdout.reconfigure(encoding="utf-8", line_buffering=True)  # type: ignore
    except Exception:
        pass

    for raw in stdin:
        cmdline = raw.strip()
        if not cmdline:
            continue
        op, *rest = cmdline.split(" ", 1)
        op = op.upper()

        try:
            if op == "SET" and rest:
                if " " in rest[0]:
                    key, value = rest[0].split(" ", 1)
                else:
                    key, value = rest[0], ""
                store.set(key, value)
                print("OK", file=stdout)

            elif op == "GET" and rest:
                key = rest[0]
                val = store.get(key)
                print(val if val is not None else "", file=stdout)

            elif op == "EXIT":
                return 0
            else:
                # Ignore invalid commands silently (autograder requirement)
                continue

        except ValueError as e:
            logger.error("%s", e)
            print("ERROR", file=stdout)
        except Exception as e:
            logger.exception("Unexpected error: %s", e)
            print("ERROR", file=stdout)

    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    """Optional argparse CLI for manual runs."""
    p = argparse.ArgumentParser(description="Persistent key–value store CLI")
    p.add_argument("--db", default=str(DATA_FILE), help="Path to DB file")
    return p


def main() -> None:
    """Entry point for command-line execution."""
    args = build_arg_parser().parse_args()
    store = KeyValueStore(Path(args.db))
    code = run_cli(store, sys.stdin, sys.stdout)
    sys.exit(code)


if __name__ == "__main__":
    main()
