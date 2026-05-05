"""
db_manager.py — 统一数据库管理层
==================================
负责所有 SQLite 表的创建、数据插入、去重和日志记录。
融合了智研TRACK 的任务系统 + 哈希去重 + 运行日志。

迁移自 Academic_Scraper_Ultimate，改为包内相对导入。
"""

import sqlite3
import hashlib
import re
from datetime import datetime
from .config import PAPERS_DB_PATH, TASKS_DB_PATH, WOS_BIBTEX_FIELDS


# ============================================================
#  工具函数
# ============================================================

def generate_hash(title: str, authors: str, journal: str, date: str) -> str:
    """基于核心元数据生成唯一指纹，用于去重"""
    content = f"{title or ''}|{authors or ''}|{journal or ''}|{date or ''}"
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def sanitize_column_name(name: str) -> str:
    """将 BibTeX 字段名转换为 SQL 安全列名"""
    name = str(name).replace("-", "_").replace(" ", "_").replace(".", "_")
    name = re.sub(r"[^\w]", "", name)
    if name and name[0].isdigit():
        name = f"col_{name}"
    return name or "unknown_field"


# 预计算 WoS BibTeX 字段到 SQL 列的映射
WOS_FIELD_TO_COL = {f: sanitize_column_name(f) for f in WOS_BIBTEX_FIELDS}
WOS_SQL_COLUMNS = sorted(set(WOS_FIELD_TO_COL.values()))


# ============================================================
#  连接管理
# ============================================================

def get_papers_db() -> sqlite3.Connection:
    """获取文献数据库连接"""
    conn = sqlite3.connect(PAPERS_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")  # 并发读写优化
    return conn


def get_tasks_db() -> sqlite3.Connection:
    """获取任务调度数据库连接"""
    conn = sqlite3.connect(TASKS_DB_PATH)
    return conn


# ============================================================
#  任务表 (tasks.db)
# ============================================================

def init_tasks_table(conn: sqlite3.Connection):
    """创建任务表"""
    conn.execute("""
    CREATE TABLE IF NOT EXISTS search_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        platform TEXT NOT NULL DEFAULT 'cnki',
        query TEXT NOT NULL,
        source_key TEXT DEFAULT '',
        source_name TEXT DEFAULT '',
        actual_total_results INTEGER DEFAULT 0,
        run_params TEXT,
        created_at DATETIME DEFAULT (datetime('now', 'localtime'))
    )
    """)
    _ensure_column(
        conn,
        "search_tasks",
        "actual_total_results",
        "INTEGER DEFAULT 0",
    )
    conn.commit()


def add_task(conn: sqlite3.Connection, platform: str, query: str,
             source_key: str = "", source_name: str = "") -> int:
    """添加一个检索任务，返回任务 ID"""
    cursor = conn.execute(
        "INSERT INTO search_tasks (platform, query, source_key, source_name) VALUES (?, ?, ?, ?)",
        (platform, query, source_key, source_name),
    )
    conn.commit()
    return cursor.lastrowid


def get_tasks(conn: sqlite3.Connection, platform: str = None) -> list:
    """获取任务列表，可按平台过滤"""
    if platform:
        cursor = conn.execute(
            "SELECT id, platform, query, source_key, source_name FROM search_tasks WHERE lower(platform) = ?",
            (platform.lower(),),
        )
    else:
        cursor = conn.execute(
            "SELECT id, platform, query, source_key, source_name FROM search_tasks"
        )
    rows = cursor.fetchall()
    return [
        {"id": r[0], "platform": r[1], "query": r[2],
         "source_key": r[3], "source_name": r[4]}
        for r in rows
    ]


def update_task_total(conn: sqlite3.Connection, task_id: int, total: int):
    """更新任务的实际检索结果总数"""
    init_tasks_table(conn)
    conn.execute(
        "UPDATE search_tasks SET actual_total_results = ? WHERE id = ?",
        (total, task_id),
    )
    conn.commit()


def _ensure_column(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_def: str,
):
    """Ensure an additive schema migration is applied for existing SQLite DBs."""
    columns = {
        row[1]
        for row in conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    }
    if column_name not in columns:
        conn.execute(
            f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_def}'
        )


# ============================================================
#  运行日志表 (papers.db)
# ============================================================

def init_run_log_table(conn: sqlite3.Connection):
    """创建运行日志表"""
    conn.execute("""
    CREATE TABLE IF NOT EXISTS run_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        platform TEXT NOT NULL,
        query TEXT NOT NULL,
        total_found INTEGER DEFAULT 0,
        new_added INTEGER DEFAULT 0,
        status TEXT DEFAULT 'running',
        run_at DATETIME DEFAULT (datetime('now', 'localtime'))
    )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_run_logs_task ON run_logs (task_id)")
    conn.commit()


def log_run(conn: sqlite3.Connection, task_id: int, platform: str,
            query: str, total_found: int, new_added: int, status: str):
    """记录一次爬取运行"""
    conn.execute(
        """INSERT INTO run_logs (task_id, platform, query, total_found, new_added, status, run_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            task_id,
            platform,
            query,
            total_found,
            new_added,
            status,
            datetime.now().isoformat(sep=" ", timespec="seconds"),
        ),
    )
    conn.commit()


# ============================================================
#  CNKI 数据表 (papers.db, 按任务隔离)
# ============================================================

def init_cnki_table(conn: sqlite3.Connection, table_name: str):
    """为单个 CNKI 任务创建数据表"""
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        title TEXT,
        authors TEXT,
        journal TEXT,
        publish_date TEXT,
        cited_count INTEGER DEFAULT 0,
        download_count INTEGER DEFAULT 0,
        detail_url TEXT UNIQUE,
        abstract TEXT,
        keywords TEXT,
        data_hash TEXT UNIQUE,
        first_seen_at DATETIME DEFAULT (datetime('now', 'localtime')),
        last_seen_at DATETIME DEFAULT (datetime('now', 'localtime'))
    )
    """)
    conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_hash" ON "{table_name}" (data_hash)')
    conn.commit()


def cnki_paper_exists(conn: sqlite3.Connection, data_hash: str, table_name: str) -> bool:
    """检查论文是否已存在 (基于哈希)"""
    cursor = conn.execute(f'SELECT 1 FROM "{table_name}" WHERE data_hash = ?', (data_hash,))
    return cursor.fetchone() is not None


def insert_cnki_paper(conn: sqlite3.Connection, paper: dict, table_name: str) -> bool:
    """插入一条 CNKI 论文记录，冲突时更新 last_seen_at"""
    try:
        conn.execute(f"""
        INSERT INTO "{table_name}" (
            task_id, title, authors, journal, publish_date,
            cited_count, download_count, detail_url, abstract, keywords,
            data_hash, last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            paper["task_id"], paper["title"], paper["authors"],
            paper["journal"], paper["publish_date"],
            paper.get("cited_count", 0), paper.get("download_count", 0),
            paper["detail_url"], paper["abstract"], paper.get("keywords", ""),
            paper["data_hash"], datetime.now(),
        ))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        # 已存在则更新 last_seen_at
        conn.execute(
            f'UPDATE "{table_name}" SET last_seen_at = ? WHERE data_hash = ?',
            (datetime.now(), paper["data_hash"]),
        )
        conn.commit()
        return False


# ============================================================
#  WoS 数据表 (papers.db, 按任务隔离)
# ============================================================

def init_wos_table(conn: sqlite3.Connection, table_name: str):
    """为单个 WoS 任务创建数据表 (BibTeX 全字段)"""
    cols_sql = ",\n".join([f'"{c}" TEXT' for c in WOS_SQL_COLUMNS])
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        db_id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        wos_id TEXT UNIQUE NOT NULL,
        {cols_sql},
        first_seen_at DATETIME DEFAULT (datetime('now', 'localtime')),
        last_seen_at DATETIME DEFAULT (datetime('now', 'localtime'))
    )
    """)
    conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_wos_id" ON "{table_name}" (wos_id)')
    conn.commit()


def init_wos_quick_table(conn: sqlite3.Connection, table_name: str):
    """为 WoS 快速模式 (Zero-Jump 列表提取) 创建轻量数据表"""
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        title TEXT,
        journal TEXT,
        abstract TEXT,
        data_hash TEXT UNIQUE,
        first_seen_at DATETIME DEFAULT (datetime('now', 'localtime')),
        last_seen_at DATETIME DEFAULT (datetime('now', 'localtime'))
    )
    """)
    conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_hash" ON "{table_name}" (data_hash)')
    conn.commit()


def insert_wos_bibtex_paper(conn: sqlite3.Connection, entry: dict,
                            table_name: str, task_id: int) -> bool:
    """插入一条 BibTeX 解析后的 WoS 论文记录"""
    wos_id = entry.get("ID")
    if not wos_id:
        return False

    db_columns = ["wos_id", "task_id"] + WOS_SQL_COLUMNS
    values = [wos_id, task_id]

    for sql_col in WOS_SQL_COLUMNS:
        # 反向查找原始 BibTeX 字段名
        orig_field = next((k for k, v in WOS_FIELD_TO_COL.items() if v == sql_col), None)
        raw = entry.get(orig_field.lower()) if orig_field else None
        if raw is not None:
            if orig_field == "Abstract":
                raw = " ".join(str(raw).splitlines())
            elif orig_field == "Journal":
                raw = str(raw).replace("/", "")
            else:
                raw = str(raw)
        values.append(raw)

    placeholders = ", ".join(["?"] * len(db_columns))
    cols_str = ", ".join([f'"{c}"' for c in db_columns])

    # ON CONFLICT 更新所有数据列 + last_seen_at
    update_parts = [f'"{c}" = excluded."{c}"' for c in WOS_SQL_COLUMNS]
    update_parts.append("task_id = excluded.task_id")
    update_parts.append("last_seen_at = datetime('now', 'localtime')")
    update_clause = ", ".join(update_parts)

    try:
        conn.execute(f"""
        INSERT INTO "{table_name}" ({cols_str}) VALUES ({placeholders})
        ON CONFLICT(wos_id) DO UPDATE SET {update_clause}
        """, values)
        conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"  [DB Error] WoS 插入失败 (wos_id={wos_id}): {e}")
        conn.rollback()
        return False


def insert_wos_quick_paper(conn: sqlite3.Connection, paper: dict, table_name: str) -> bool:
    """插入一条 WoS 快速模式记录"""
    try:
        conn.execute(f"""
        INSERT INTO "{table_name}" (task_id, title, journal, abstract, data_hash, last_seen_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (
            paper["task_id"], paper["title"], paper["journal"],
            paper["abstract"], paper["data_hash"], datetime.now(),
        ))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        conn.execute(
            f'UPDATE "{table_name}" SET last_seen_at = ? WHERE data_hash = ?',
            (datetime.now(), paper["data_hash"]),
        )
        conn.commit()
        return False
