import sqlite3

from xiaoyan.scraper.db_manager import (
    add_task,
    init_tasks_table,
    log_run,
    update_task_total,
)


def test_init_tasks_table_adds_total_results_column_to_existing_db():
    conn = sqlite3.connect(":memory:")
    conn.execute("""
    CREATE TABLE search_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        platform TEXT NOT NULL DEFAULT 'cnki',
        query TEXT NOT NULL,
        source_key TEXT DEFAULT '',
        source_name TEXT DEFAULT '',
        run_params TEXT,
        created_at DATETIME DEFAULT (datetime('now', 'localtime'))
    )
    """)

    init_tasks_table(conn)

    columns = {
        row[1]
        for row in conn.execute('PRAGMA table_info("search_tasks")').fetchall()
    }
    assert "actual_total_results" in columns


def test_update_task_total_persists_total_results():
    conn = sqlite3.connect(":memory:")
    init_tasks_table(conn)
    task_id = add_task(conn, "cnki", "SU='测试'")

    update_task_total(conn, task_id, 42)

    total = conn.execute(
        "SELECT actual_total_results FROM search_tasks WHERE id = ?",
        (task_id,),
    ).fetchone()[0]
    assert total == 42


def test_log_run_writes_single_run_log_row():
    conn = sqlite3.connect(":memory:")
    from xiaoyan.scraper.db_manager import init_run_log_table

    init_run_log_table(conn)
    log_run(conn, 1, "cnki", "SU='测试'", 10, 3, "success")

    row = conn.execute(
        "SELECT task_id, platform, total_found, new_added, status FROM run_logs"
    ).fetchone()
    assert row == (1, "cnki", 10, 3, "success")
