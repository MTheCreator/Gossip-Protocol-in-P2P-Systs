"""
components/storage.py  —  Disk Persistence via SQLite
======================================================
Provides crash-recovery for:
  - Sequenced messages (per channel, keyed by seq_num)
  - Channel state (last delivered seq, subscription flag)
  - Neighbour list (for fast re-join after restart)

Uses Python's built-in sqlite3 — no external dependencies.
All mutations are serialised through a single threading.Lock.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class MessageStore:
    """
    SQLite-backed persistence for gossip protocol state.

    Tables
    ------
    messages      : (channel TEXT, seq_num INT, msg_json TEXT, delivered_at REAL)
                    PK = (channel, seq_num)  — idempotent writes via INSERT OR IGNORE
    channel_state : (channel TEXT PK, last_delivered_seq INT, subscribed INT)
    node_state    : (key TEXT PK, value_json TEXT)  — generic KV for peers, config, etc.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = threading.Lock()

        # Ensure parent directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._create_tables()
        logger.info("[Storage] Opened database at %s", db_path)

    def _create_tables(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    channel      TEXT    NOT NULL,
                    seq_num      INTEGER NOT NULL,
                    msg_json     TEXT    NOT NULL,
                    delivered_at REAL,
                    PRIMARY KEY (channel, seq_num)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS channel_state (
                    channel            TEXT PRIMARY KEY,
                    last_delivered_seq INTEGER NOT NULL DEFAULT 0,
                    subscribed         INTEGER NOT NULL DEFAULT 0
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS node_state (
                    key        TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL
                )
            """)
            self._conn.commit()

    # ------------------------------------------------------------------
    # Messages
    # ------------------------------------------------------------------

    def store_message(self, channel: str, seq_num: int, msg_dict: Dict[str, Any]) -> None:
        """
        Store a sequenced message. Idempotent — duplicate (channel, seq_num)
        is silently ignored.
        """
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO messages (channel, seq_num, msg_json, delivered_at) "
                "VALUES (?, ?, ?, ?)",
                (channel, seq_num, json.dumps(msg_dict), time.time()),
            )
            self._conn.commit()

    def get_messages(
        self, channel: str, from_seq: int, to_seq: int
    ) -> List[Dict[str, Any]]:
        """
        Retrieve messages in [from_seq, to_seq] range, ordered by seq_num.
        """
        with self._lock:
            cur = self._conn.execute(
                "SELECT msg_json FROM messages "
                "WHERE channel = ? AND seq_num >= ? AND seq_num <= ? "
                "ORDER BY seq_num",
                (channel, from_seq, to_seq),
            )
            rows = cur.fetchall()
        return [json.loads(row[0]) for row in rows]

    def get_last_seq(self, channel: str) -> int:
        """Return the highest stored seq_num for a channel, or 0."""
        with self._lock:
            cur = self._conn.execute(
                "SELECT MAX(seq_num) FROM messages WHERE channel = ?",
                (channel,),
            )
            row = cur.fetchone()
        return row[0] if row and row[0] is not None else 0

    # ------------------------------------------------------------------
    # Channel state
    # ------------------------------------------------------------------

    def save_channel_state(
        self, channel: str, last_delivered_seq: int, subscribed: bool
    ) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO channel_state "
                "(channel, last_delivered_seq, subscribed) VALUES (?, ?, ?)",
                (channel, last_delivered_seq, int(subscribed)),
            )
            self._conn.commit()

    def load_channel_state(self, channel: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT last_delivered_seq, subscribed FROM channel_state "
                "WHERE channel = ?",
                (channel,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return {
            "last_delivered_seq": row[0],
            "subscribed": bool(row[1]),
        }

    def load_all_channel_states(self) -> Dict[str, Dict[str, Any]]:
        """Return {channel_name: {last_delivered_seq, subscribed}} for all channels."""
        with self._lock:
            cur = self._conn.execute(
                "SELECT channel, last_delivered_seq, subscribed FROM channel_state"
            )
            rows = cur.fetchall()
        return {
            row[0]: {"last_delivered_seq": row[1], "subscribed": bool(row[2])}
            for row in rows
        }

    # ------------------------------------------------------------------
    # Neighbour persistence
    # ------------------------------------------------------------------

    def save_neighbours(self, peers: List[Dict[str, Any]]) -> None:
        """Save current neighbour list for fast re-join on restart."""
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO node_state (key, value_json) VALUES (?, ?)",
                ("neighbours", json.dumps(peers)),
            )
            self._conn.commit()

    def load_neighbours(self) -> List[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT value_json FROM node_state WHERE key = ?",
                ("neighbours",),
            )
            row = cur.fetchone()
        if row is None:
            return []
        return json.loads(row[0])

    # ------------------------------------------------------------------
    # Generic KV (for future use)
    # ------------------------------------------------------------------

    def save_state(self, key: str, value: Any) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO node_state (key, value_json) VALUES (?, ?)",
                (key, json.dumps(value)),
            )
            self._conn.commit()

    def load_state(self, key: str) -> Optional[Any]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT value_json FROM node_state WHERE key = ?", (key,)
            )
            row = cur.fetchone()
        return json.loads(row[0]) if row else None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        with self._lock:
            self._conn.close()
        logger.info("[Storage] Database closed")
