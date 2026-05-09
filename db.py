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
    status          TEXT NOT NULL DEFAULT 'running',
    leads_found     INTEGER NOT NULL DEFAULT 0,
    active_leads    INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at     TEXT,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS search_leads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    search_id       INTEGER NOT NULL,
    user_id         INTEGER NOT NULL,
    username        TEXT NOT NULL,
    x_profile       TEXT,
    bio_snippet     TEXT,
    shopify_url     TEXT,
    is_shopify      INTEGER NOT NULL DEFAULT 0,
    is_active       INTEGER NOT NULL DEFAULT 0,
    active_reason   TEXT,
    category        TEXT,
    signals         TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (search_id) REFERENCES searches(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_searches_user ON searches(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_leads_user ON search_leads(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_leads_search ON search_leads(search_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_leads_search_user ON search_leads(search_id, username);
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
        # Migrate: add columns if upgrading an older DB
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(searches)").fetchall()}
        if "status" not in cols:
            conn.execute("ALTER TABLE searches ADD COLUMN status TEXT NOT NULL DEFAULT 'done'")
        if "finished_at" not in cols:
            conn.execute("ALTER TABLE searches ADD COLUMN finished_at TEXT")


def create_user(email: str, password_hash: str) -> int:
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO users (email, password_hash) VALUES (?, ?)",
            (email.lower().strip(), password_hash),
        )
        return cur.lastrowid


def update_password(user_id: int, password_hash: str) -> None:
    with get_db() as conn:
        conn.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (password_hash, user_id),
        )


def delete_user(user_id: int) -> None:
    """GDPR-style account deletion: removes user + all associated data."""
    with get_db() as conn:
        conn.execute("DELETE FROM search_leads WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM searches WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))


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


def add_search_lead(search_id: int, user_id: int, lead: dict) -> None:
    """Persist a verified lead. Idempotent — duplicate (search_id, username) ignored."""
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO search_leads "
            "(search_id, user_id, username, x_profile, bio_snippet, shopify_url, "
            " is_shopify, is_active, active_reason, category, signals) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                search_id, user_id, lead.get("username", ""),
                lead.get("x_profile", ""),
                (lead.get("bio_snippet") or "")[:1000],
                lead.get("shopify_url", ""),
                1 if lead.get("shopify") else 0,
                1 if lead.get("active") else 0,
                lead.get("active_reason", ""),
                lead.get("category", ""),
                lead.get("signals", ""),
            ),
        )


def update_search_status(search_id: int, user_id: int, status: str) -> None:
    finished_at = "datetime('now')" if status in ("done", "cancelled", "error") else "NULL"
    with get_db() as conn:
        conn.execute(
            f"UPDATE searches SET status = ?, finished_at = {finished_at} "
            "WHERE id = ? AND user_id = ?",
            (status, search_id, user_id),
        )


def get_running_search(user_id: int) -> dict | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM searches WHERE user_id = ? AND status = 'running' "
            "ORDER BY created_at DESC LIMIT 1",
            (user_id,),
        ).fetchone()
        return dict(row) if row else None


def get_leads_for_search(search_id: int, user_id: int, active_only: bool = False) -> list[dict]:
    where = "search_id = ? AND user_id = ?"
    params = (search_id, user_id)
    if active_only:
        where += " AND is_active = 1"
    with get_db() as conn:
        rows = conn.execute(
            f"SELECT * FROM search_leads WHERE {where} "
            "ORDER BY is_active DESC, created_at DESC",
            params,
        ).fetchall()
        return [dict(r) for r in rows]


def get_recent_user_leads(user_id: int, limit: int = 200, active_only: bool = False) -> list[dict]:
    where = "user_id = ?"
    if active_only:
        where += " AND is_active = 1"
    with get_db() as conn:
        rows = conn.execute(
            f"SELECT * FROM search_leads WHERE {where} "
            "ORDER BY is_active DESC, created_at DESC LIMIT ?",
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
