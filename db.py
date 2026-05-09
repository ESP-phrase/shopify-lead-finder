"""SQLite layer for users, sessions, search history, quotas."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "lead_finder.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    email           TEXT UNIQUE NOT NULL,
    password_hash   TEXT NOT NULL,
    plan            TEXT NOT NULL DEFAULT 'free',
    credits         INTEGER NOT NULL DEFAULT 5,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS searches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL,
    keywords        TEXT,
    engine          TEXT,
    broad           INTEGER NOT NULL DEFAULT 0,
    leads_found     INTEGER NOT NULL DEFAULT 0,
    active_leads    INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_searches_user ON searches(user_id, created_at);
"""


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with get_db() as conn:
        conn.executescript(SCHEMA)


def create_user(email: str, password_hash: str = "") -> int:
    """Create a user. password_hash defaults to '' for fake auth."""
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO users (email, password_hash) VALUES (?, ?)",
            (email.lower().strip(), password_hash),
        )
        return cur.lastrowid


def get_or_create_user(email: str) -> dict:
    """Fake-auth helper: find by email or create with 5 free credits."""
    existing = get_user_by_email(email)
    if existing:
        return existing
    user_id = create_user(email)
    return get_user(user_id)


def get_user_by_email(email: str) -> dict | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM users WHERE email = ?",
            (email.lower().strip(),),
        ).fetchone()
        return dict(row) if row else None


def get_user(user_id: int) -> dict | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM users WHERE id = ?", (user_id,)
        ).fetchone()
        return dict(row) if row else None


def use_credit(user_id: int) -> bool:
    """Decrement one credit if available. Returns True if a credit was used."""
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE users SET credits = credits - 1 "
            "WHERE id = ? AND credits > 0",
            (user_id,),
        )
        return cur.rowcount > 0


def add_credits(user_id: int, n: int) -> None:
    with get_db() as conn:
        conn.execute(
            "UPDATE users SET credits = credits + ? WHERE id = ?",
            (n, user_id),
        )


def log_search(
    user_id: int, keywords: str, engine: str, broad: bool,
    leads_found: int, active_leads: int,
) -> None:
    with get_db() as conn:
        conn.execute(
            "INSERT INTO searches "
            "(user_id, keywords, engine, broad, leads_found, active_leads) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, keywords, engine, int(broad), leads_found, active_leads),
        )


def update_search_results(user_id: int, search_id: int, leads_found: int, active_leads: int) -> None:
    with get_db() as conn:
        conn.execute(
            "UPDATE searches SET leads_found = ?, active_leads = ? "
            "WHERE id = ? AND user_id = ?",
            (leads_found, active_leads, search_id, user_id),
        )


def insert_search_start(user_id: int, keywords: str, engine: str, broad: bool) -> int:
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO searches (user_id, keywords, engine, broad) VALUES (?, ?, ?, ?)",
            (user_id, keywords, engine, int(broad)),
        )
        return cur.lastrowid


def get_recent_searches(user_id: int, limit: int = 20) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM searches WHERE user_id = ? "
            "ORDER BY created_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def user_stats(user_id: int) -> dict:
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS searches, "
            "       COALESCE(SUM(active_leads), 0) AS total_active, "
            "       COALESCE(SUM(leads_found), 0) AS total_found "
            "FROM searches WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        return dict(row)
