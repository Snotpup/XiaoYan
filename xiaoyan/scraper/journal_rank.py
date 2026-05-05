"""
journal_rank.py — 期刊分区匹配模块
====================================
迁移自智研TRACK 分区查询.py，提供:
  - JCR 数据库加载 (中科院大类 + 分区)
  - 期刊名模糊匹配 (仅保留小写字母, 去除标点/空格/特殊符号)
  - WoS 论文批量分区填充

用法:
  from xiaoyan.scraper.journal_rank import load_jcr_data, enrich_wos_papers
  jcr = load_jcr_data()
  enrich_wos_papers("wos_deep_42", jcr)
"""

import logging
import os
import re
import sqlite3

from .config import (
    JCR_DB_PATH, JCR_TABLE_NAME,
    JCR_JOURNAL_COL, JCR_DALEI_COL, JCR_FENQU_COL,
    PAPERS_DB_PATH,
)

logger = logging.getLogger("xiaoyan.scraper.journal_rank")


# ============================================================
#  规范化匹配 (迁移自智研TRACK 分区查询.py L24-36)
# ============================================================

def normalize_journal_name(name: str) -> str:
    """
    规范化期刊名以便匹配：
    1. 转换为小写
    2. 只保留英文字母 (a-z)，去除空格、标点、数字

    示例: "China World Economy" → "chinaworldeconomy"
          "J. Finance" → "jfinance"
    """
    if not name or not isinstance(name, str):
        return ""
    return re.sub(r"[^a-z]", "", name.lower())


# ============================================================
#  JCR 数据加载
# ============================================================

def load_jcr_data(jcr_db_path: str = None) -> dict:
    """
    从 JCR 数据库加载期刊→分区映射。

    Returns:
        dict: {normalized_name: {"dalei": str, "fenqu": str, "raw_name": str}}
    """
    db_path = jcr_db_path or JCR_DB_PATH

    if not os.path.exists(db_path):
        logger.warning(f"JCR 数据库不存在: {db_path}")
        logger.info(
            "请将 jcr.db 放到 ~/.xiaoyan/scraper/ 下，"
            "或设置 XIAOYAN_JCR_DB_PATH 环境变量"
        )
        return {}

    jcr_map = {}
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        query = (
            f'SELECT "{JCR_JOURNAL_COL}", "{JCR_DALEI_COL}", "{JCR_FENQU_COL}" '
            f'FROM "{JCR_TABLE_NAME}"'
        )
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            raw_name = row[JCR_JOURNAL_COL]
            dalei = row[JCR_DALEI_COL]
            fenqu_raw = row[JCR_FENQU_COL]

            if not raw_name:
                continue

            # 解析分区 (去除可能的后缀标记, 如 "1[Top]" → "1")
            fenqu = ""
            if fenqu_raw:
                parts = str(fenqu_raw).split("[", 1)
                fenqu = parts[0].strip()

            normalized = normalize_journal_name(raw_name)
            if normalized:
                jcr_map[normalized] = {
                    "dalei": dalei or "",
                    "fenqu": fenqu,
                    "raw_name": raw_name,
                }

        logger.info(f"JCR 数据已加载: {len(jcr_map)} 条期刊映射")

    except sqlite3.Error as e:
        logger.error(f"JCR 数据库读取失败: {e}")
    finally:
        if conn:
            conn.close()

    return jcr_map


# ============================================================
#  WoS 论文分区匹配
# ============================================================

def match_journal(journal_name: str, jcr_data: dict) -> dict | None:
    """
    单篇论文的期刊名匹配。

    Returns:
        {"dalei": str, "fenqu": str} 或 None (未匹配)
    """
    if not journal_name or not jcr_data:
        return None

    normalized = normalize_journal_name(journal_name)
    if not normalized:
        return None

    entry = jcr_data.get(normalized)
    if entry:
        return {"dalei": entry["dalei"], "fenqu": entry["fenqu"]}
    return None


def enrich_wos_papers(
    table_name: str,
    jcr_data: dict,
    papers_db_path: str = None,
) -> tuple[int, int]:
    """
    批量匹配并更新 WoS 论文表的中科院分区字段。

    Args:
        table_name: WoS 数据表名 (如 "wos_deep_42")
        jcr_data: load_jcr_data() 返回的映射字典
        papers_db_path: 文献数据库路径 (默认取 config)

    Returns:
        (matched_count, total_count)
    """
    db_path = papers_db_path or PAPERS_DB_PATH

    if not jcr_data:
        logger.warning("JCR 数据为空，跳过分区匹配")
        return 0, 0

    conn = None
    matched = 0
    total = 0

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 确保分区列存在 (动态 ALTER TABLE)
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        existing_cols = {row[1] for row in cursor.fetchall()}

        for col_name in ["中科院大类", "中科院分区"]:
            if col_name not in existing_cols:
                cursor.execute(
                    f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" TEXT'
                )
                logger.info(f'已添加列 "{col_name}" 到表 "{table_name}"')

        # 读取所有论文的期刊名
        cursor.execute(f'SELECT rowid, "Journal" FROM "{table_name}"')
        rows = cursor.fetchall()
        total = len(rows)

        for rowid, journal_name in rows:
            result = match_journal(journal_name, jcr_data)
            dalei = result["dalei"] if result else None
            fenqu = result["fenqu"] if result else None

            cursor.execute(
                f'UPDATE "{table_name}" '
                f'SET "中科院大类" = ?, "中科院分区" = ? '
                f'WHERE rowid = ?',
                (dalei, fenqu, rowid),
            )

            if result:
                matched += 1

        conn.commit()
        logger.info(
            f'表 "{table_name}" 分区匹配完成: '
            f'{matched}/{total} 篇成功匹配'
        )

    except sqlite3.Error as e:
        logger.error(f'分区匹配失败 (表 "{table_name}"): {e}')
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

    return matched, total
