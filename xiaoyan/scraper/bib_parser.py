"""
bib_parser.py — 独立 BibTeX → SQLite 入库工具
================================================
迁移自智研TRACK bib转换db.py，提供:
  - 独立 CLI 入口: python -m xiaoyan.scraper.bib_parser /path/to/file.bib
  - 可编程接口: from xiaoyan.scraper.bib_parser import parse_bib_file

核心特性:
  ✅ 自动建表 (wos_deep_{task_id})
  ✅ UPSERT 去重 (wos_id 冲突时更新)
  ✅ 支持批量 .bib 文件 (目录扫描)
  ✅ 可选 JCR 分区自动匹配

用法:
  # 单文件导入
  python -m xiaoyan.scraper.bib_parser file.bib --task-id 42

  # 整个目录导入
  python -m xiaoyan.scraper.bib_parser ./wos_downloads/ --task-id 42

  # 导入并匹配 JCR 分区
  python -m xiaoyan.scraper.bib_parser file.bib --task-id 42 --jcr
"""

import argparse
import os
import sys

from .db_manager import (
    get_papers_db, init_wos_table,
    insert_wos_bibtex_paper,
)


def parse_bib_file(filepath: str, task_id: int, conn=None) -> int:
    """
    解析单个 .bib 文件并入库。

    Args:
        filepath: .bib 文件路径
        task_id: 任务 ID
        conn: SQLite 连接 (可选, 不提供则自动创建)

    Returns:
        成功入库的记录数
    """
    try:
        import bibtexparser
    except ImportError:
        print("⚠ bibtexparser 未安装")
        print("  安装命令: pip install bibtexparser")
        return 0

    if not os.path.exists(filepath):
        print(f"✗ 文件不存在: {filepath}")
        return 0

    should_close = False
    if conn is None:
        conn = get_papers_db()
        should_close = True

    table_name = f"wos_deep_{task_id}"
    init_wos_table(conn, table_name)

    count = 0
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            bib_db = bibtexparser.load(f)

        entries = bib_db.entries
        print(f"  📄 {os.path.basename(filepath)}: {len(entries)} 条记录")

        for entry in entries:
            if insert_wos_bibtex_paper(conn, entry, table_name, task_id):
                count += 1

        print(f"  ✓ 入库成功: {count}/{len(entries)} 条")

        # 入库成功后自动删除源文件
        if count > 0:
            try:
                os.remove(filepath)
                print(f"  🗑 已删除源文件: {os.path.basename(filepath)}")
            except OSError as rm_err:
                print(f"  ⚠ 删除源文件失败: {rm_err}")

    except Exception as e:
        print(f"  ✗ 解析错误: {e}")

    if should_close:
        conn.close()

    return count


def parse_bib_directory(dirpath: str, task_id: int) -> int:
    """
    扫描目录下所有 .bib 文件并入库。

    Args:
        dirpath: 目录路径
        task_id: 任务 ID

    Returns:
        总入库记录数
    """
    if not os.path.isdir(dirpath):
        print(f"✗ 目录不存在: {dirpath}")
        return 0

    bib_files = sorted([
        os.path.join(dirpath, f)
        for f in os.listdir(dirpath)
        if f.endswith(".bib")
    ])

    if not bib_files:
        print(f"✗ 目录中没有 .bib 文件: {dirpath}")
        return 0

    conn = get_papers_db()
    total = 0

    print(f"\n{'=' * 50}")
    print(f"  BibTeX 批量导入")
    print(f"  目录: {dirpath}")
    print(f"  文件数: {len(bib_files)}")
    print(f"  任务 ID: {task_id}")
    print(f"{'=' * 50}\n")

    for filepath in bib_files:
        count = parse_bib_file(filepath, task_id, conn)
        total += count

    conn.close()

    print(f"\n{'=' * 50}")
    print(f"  导入完成: 共 {total} 条记录入库")
    print(f"{'=' * 50}\n")

    # 如果目录已空 (所有 .bib 文件都被删除), 清理空目录
    remaining = [f for f in os.listdir(dirpath) if f.endswith(".bib")]
    if not remaining:
        try:
            os.rmdir(dirpath)
            print(f"  🗑 已清理空目录: {dirpath}")
        except OSError:
            pass  # 目录非空 (含其他文件), 保留

    return total


def main():
    """CLI 入口"""
    parser = argparse.ArgumentParser(
        description="BibTeX → SQLite 入库工具 (迁移自智研TRACK bib转换db.py)",
        prog="python -m xiaoyan.scraper.bib_parser",
    )
    parser.add_argument(
        "path",
        help="单个 .bib 文件路径, 或包含多个 .bib 文件的目录路径",
    )
    parser.add_argument(
        "--task-id", "-t",
        type=int,
        required=True,
        help="任务 ID (用于数据表命名: wos_deep_{task_id})",
    )
    parser.add_argument(
        "--jcr",
        action="store_true",
        help="入库后自动匹配 JCR 期刊分区 (需要 jcr.db)",
    )

    args = parser.parse_args()

    # 执行导入
    if os.path.isdir(args.path):
        total = parse_bib_directory(args.path, args.task_id)
    elif os.path.isfile(args.path):
        total = parse_bib_file(args.path, args.task_id)
    else:
        print(f"✗ 路径不存在: {args.path}")
        sys.exit(1)

    # 可选: JCR 分区匹配
    if args.jcr and total > 0:
        print("\n📊 正在匹配 JCR 期刊分区...")
        try:
            from .journal_rank import load_jcr_data, enrich_wos_papers
            jcr_data = load_jcr_data()
            if jcr_data:
                table_name = f"wos_deep_{args.task_id}"
                matched, total_rows = enrich_wos_papers(table_name, jcr_data)
                print(f"  ✓ 分区匹配: {matched}/{total_rows} 篇")
            else:
                print("  ⚠ JCR 数据库未找到, 跳过分区匹配")
        except Exception as e:
            print(f"  ⚠ 分区匹配失败: {e}")

    print(f"\n完成! 共 {total} 条记录入库。")


if __name__ == "__main__":
    main()
