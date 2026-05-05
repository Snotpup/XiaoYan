"""
xiaoyan.core.db — XiaoYan 自身数据库管理
==========================================
管理 XiaoYan 编排层的数据：检索式任务、推送记录等。
与 Scraper 的 db_manager.py 独立，各管各的。

表结构:
  - search_queries: 用户定时检索式 (CRUD 由飞书指令驱动)
  - push_logs: 推送记录 (去重：同一篇文献只推送一次)
"""

import sqlite3
from datetime import datetime
from pathlib import Path

from xiaoyan.config import DB_PATH


# ============================================================
#  连接管理
# ============================================================

def get_db() -> sqlite3.Connection:
    """获取 XiaoYan 主数据库连接"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """初始化所有表"""
    conn = get_db()
    _init_search_queries_table(conn)
    _init_push_logs_table(conn)
    _init_settings_table(conn)
    conn.close()


# ============================================================
#  检索式任务表
# ============================================================

def _init_search_queries_table(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS search_queries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        platform TEXT NOT NULL DEFAULT 'wos',
        query TEXT NOT NULL,
        description TEXT,
        cron_expression TEXT DEFAULT '0 */6 * * *',
        source_filters TEXT DEFAULT '',
        is_active BOOLEAN DEFAULT 1,
        created_at DATETIME DEFAULT (datetime('now', 'localtime')),
        updated_at DATETIME DEFAULT (datetime('now', 'localtime'))
    )
    """)
    conn.commit()


def add_search_query(
    platform: str,
    query: str,
    description: str = "",
    cron_expression: str = "0 */6 * * *",
    source_filters: str = "",
) -> int:
    """添加一条检索式，返回 ID"""
    conn = get_db()
    cursor = conn.execute(
        """INSERT INTO search_queries
           (platform, query, description, cron_expression, source_filters)
           VALUES (?, ?, ?, ?, ?)""",
        (platform, query, description, cron_expression, source_filters),
    )
    conn.commit()
    query_id = cursor.lastrowid
    conn.close()
    return query_id


def update_search_query(query_id: int, **kwargs) -> bool:
    """更新检索式的指定字段"""
    allowed = {"platform", "query", "description", "cron_expression",
               "source_filters", "is_active"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return False

    updates["updated_at"] = datetime.now().isoformat()
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [query_id]

    conn = get_db()
    cursor = conn.execute(
        f"UPDATE search_queries SET {set_clause} WHERE id = ?",
        values,
    )
    conn.commit()
    affected = cursor.rowcount
    conn.close()
    return affected > 0


def delete_search_query(query_id: int) -> bool:
    """删除一条检索式"""
    conn = get_db()
    cursor = conn.execute(
        "DELETE FROM search_queries WHERE id = ?", (query_id,)
    )
    conn.commit()
    affected = cursor.rowcount
    conn.close()
    return affected > 0


def list_search_queries(active_only: bool = True) -> list[dict]:
    """列出检索式"""
    conn = get_db()
    if active_only:
        rows = conn.execute(
            "SELECT * FROM search_queries WHERE is_active = 1 ORDER BY id"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM search_queries ORDER BY id"
        ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_search_query(query_id: int) -> dict | None:
    """获取单条检索式"""
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM search_queries WHERE id = ?", (query_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


# ============================================================
#  推送记录表 (防重复推送)
# ============================================================

def _init_push_logs_table(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS push_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        paper_hash TEXT NOT NULL,
        platform TEXT NOT NULL,
        query_id INTEGER,
        pushed_at DATETIME DEFAULT (datetime('now', 'localtime')),
        UNIQUE(paper_hash)
    )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_push_logs_hash ON push_logs (paper_hash)"
    )
    conn.commit()


def is_paper_pushed(paper_hash: str) -> bool:
    """检查论文是否已推送过"""
    conn = get_db()
    row = conn.execute(
        "SELECT 1 FROM push_logs WHERE paper_hash = ?", (paper_hash,)
    ).fetchone()
    conn.close()
    return row is not None


def mark_paper_pushed(paper_hash: str, platform: str, query_id: int = None):
    """标记论文已推送"""
    conn = get_db()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO push_logs (paper_hash, platform, query_id) VALUES (?, ?, ?)",
            (paper_hash, platform, query_id),
        )
        conn.commit()
    finally:
        conn.close()


# ============================================================
#  全局设置表 (飞书交互式配置)
# ============================================================

_DEFAULT_SETTINGS = {
    "default_cron": "0 */6 * * *",
    "cnki_max_pages": "3",
    "wos_max_records": "20",
    "daily_summary_time": "21:00",
}


def _init_settings_table(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at DATETIME DEFAULT (datetime('now', 'localtime'))
    )
    """)
    for key, value in _DEFAULT_SETTINGS.items():
        conn.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
            (key, value),
        )
    conn.commit()


def get_setting(key: str) -> str | None:
    """读取单个设置值"""
    conn = get_db()
    row = conn.execute(
        "SELECT value FROM settings WHERE key = ?", (key,)
    ).fetchone()
    conn.close()
    return row["value"] if row else None


def set_setting(key: str, value: str):
    """写入/更新设置值"""
    conn = get_db()
    conn.execute(
        """INSERT INTO settings (key, value, updated_at) VALUES (?, ?, datetime('now', 'localtime'))
           ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at""",
        (key, value),
    )
    conn.commit()
    conn.close()


def get_all_settings() -> dict[str, str]:
    """读取所有设置"""
    conn = get_db()
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    conn.close()
    return {row["key"]: row["value"] for row in rows}


def get_setting_int(key: str, default: int) -> int:
    """读取设置并转为 int，不存在或解析失败时返回 default"""
    val = get_setting(key)
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default
