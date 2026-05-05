"""
xiaoyan.scraper.__main__ — 独立运行入口
==========================================
支持 `python -m xiaoyan.scraper` 独立执行爬取任务，
不依赖 XiaoYan 编排层。

用法:
  python -m xiaoyan.scraper cnki --query "SU=('人工智能' + '医疗')" --pages 3
  python -m xiaoyan.scraper wos --query "TS=(AI AND healthcare)" --mode quick
  python -m xiaoyan.scraper batch
  python -m xiaoyan.scraper add-task --platform cnki --query "SU='深度学习'"
"""

import argparse
import asyncio
import os
import sys

from .config import WOS_DOWNLOAD_DIR
from .db_manager import (
    get_papers_db, get_tasks_db, init_tasks_table,
    init_run_log_table, add_task, get_tasks,
)
from .cnki_ultimate import scrape_cnki
from .wos_ultimate import scrape_wos


# ============================================================
#  命令行解析
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="学术数据终极获取方案 — 统一入口 (XiaoYan Scraper 子包)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--show-browser", action="store_true", default=False,
        dest="global_show_browser",
        help="显示浏览器界面 (首次运行或需要手动验证码时使用)",
    )

    sub = parser.add_subparsers(dest="command", help="子命令")

    # --- cnki ---
    cnki_p = sub.add_parser("cnki", help="执行知网 (CNKI) 单次爬取")
    cnki_p.add_argument("--show-browser", action="store_true", default=False,
                        dest="command_show_browser",
                        help="显示浏览器界面")
    cnki_p.add_argument("--query", "-q", required=True, help="知网专业检索式")
    cnki_p.add_argument("--sources", "-s", default="", help="来源过滤，逗号分隔")
    cnki_p.add_argument("--pages", "-p", type=int, default=None, help="最大翻页数")
    cnki_p.add_argument("--task-id", type=int, default=None, help="任务 ID")

    # --- wos ---
    wos_p = sub.add_parser("wos", help="执行 Web of Science 单次爬取")
    wos_p.add_argument("--show-browser", action="store_true", default=False,
                       dest="command_show_browser",
                       help="显示浏览器界面")
    wos_p.add_argument("--query", "-q", required=True, help="WoS 高级检索式")
    wos_p.add_argument("--mode", "-m", choices=["quick", "deep"], default="quick")
    wos_p.add_argument("--records", "-r", type=int, default=20)
    wos_p.add_argument("--site", choices=["com", "cn"], default="com",
                       help="WoS 站点，默认 com 国际站")
    wos_p.add_argument("--cn-site", action="store_true",
                       help="兼容旧参数；等同于 --site cn")
    wos_p.add_argument("--task-id", type=int, default=None)

    # --- batch ---
    batch_p = sub.add_parser("batch", help="从数据库批量执行所有任务")
    batch_p.add_argument("--show-browser", action="store_true", default=False,
                         dest="command_show_browser",
                         help="显示浏览器界面")
    batch_p.add_argument("--platform", choices=["cnki", "wos"], default=None)
    batch_p.add_argument("--wos-mode", choices=["quick", "deep"], default="quick")
    batch_p.add_argument("--wos-site", choices=["com", "cn"], default="com")

    # --- add-task ---
    add_p = sub.add_parser("add-task", help="向数据库添加检索任务")
    add_p.add_argument("--platform", required=True, choices=["cnki", "wos"])
    add_p.add_argument("--query", "-q", required=True)
    add_p.add_argument("--source-key", default="")
    add_p.add_argument("--source-name", default="")

    # --- import-bib (迁移自智研TRACK bib转换db.py) ---
    bib_p = sub.add_parser("import-bib", help="将 BibTeX 文件导入 SQLite 数据库")
    bib_p.add_argument("path", help=".bib 文件或目录路径")
    bib_p.add_argument("--task-id", "-t", type=int, required=True,
                       help="任务 ID")
    bib_p.add_argument("--jcr", action="store_true",
                       help="入库后自动匹配 JCR 期刊分区")

    return parser


# ============================================================
#  命令处理
# ============================================================

async def handle_cnki(args):
    sources = [s.strip() for s in args.sources.split(",") if s.strip()] if args.sources else []
    task_id = args.task_id
    if task_id is None:
        tasks_conn = get_tasks_db()
        init_tasks_table(tasks_conn)
        task_id = add_task(tasks_conn, "cnki", args.query,
                           source_key=",".join(sources), source_name=",".join(sources))
        tasks_conn.close()
        print(f"已自动创建任务 ID: {task_id}")

    await scrape_cnki(
        task_id=task_id, query=args.query,
        source_filters=sources, max_pages=args.pages,
        show_browser=args.show_browser,
    )


async def handle_wos(args):
    task_id = args.task_id
    if task_id is None:
        tasks_conn = get_tasks_db()
        init_tasks_table(tasks_conn)
        task_id = add_task(tasks_conn, "wos", args.query)
        tasks_conn.close()
        print(f"已自动创建任务 ID: {task_id}")

    site = "cn" if args.cn_site else args.site
    await scrape_wos(
        task_id=task_id, query=args.query,
        mode=args.mode, max_records=args.records,
        show_browser=args.show_browser, site=site,
    )


async def handle_batch(args):
    tasks_conn = get_tasks_db()
    init_tasks_table(tasks_conn)
    tasks = get_tasks(tasks_conn, platform=args.platform)
    tasks_conn.close()

    if not tasks:
        print("  数据库中无待执行任务。")
        return

    print(f"\n共发现 {len(tasks)} 个任务待执行：\n")
    for t in tasks:
        print(f"  [ID {t['id']}] {t['platform'].upper():5s} | {t['query'][:60]}")

    for i, task in enumerate(tasks):
        print(f"\n{'─' * 60}")
        print(f"  执行任务 {i + 1}/{len(tasks)} (ID: {task['id']})")
        print(f"{'─' * 60}")

        if task["platform"] == "cnki":
            sources = [s.strip() for s in task["source_name"].split(",") if s.strip()]
            await scrape_cnki(
                task_id=task["id"], query=task["query"],
                source_filters=sources, show_browser=args.show_browser,
            )
        elif task["platform"] == "wos":
            await scrape_wos(
                task_id=task["id"], query=task["query"],
                mode=args.wos_mode, show_browser=args.show_browser,
                site=args.wos_site,
            )

        if i < len(tasks) - 1:
            print("  等待 5 秒后执行下一个任务...")
            await asyncio.sleep(5)

    print(f"\n✅ 全部 {len(tasks)} 个任务执行完毕。")


def handle_add_task(args):
    tasks_conn = get_tasks_db()
    init_tasks_table(tasks_conn)
    tid = add_task(tasks_conn, args.platform, args.query,
                   args.source_key, args.source_name)
    tasks_conn.close()
    print(f"✅ 任务已添加 | ID: {tid} | 平台: {args.platform} | 检索式: {args.query}")


# ============================================================
#  主入口
# ============================================================

def main():
    parser = build_parser()
    args = parser.parse_args()
    args.show_browser = (
        getattr(args, "global_show_browser", False)
        or getattr(args, "command_show_browser", False)
    )

    os.makedirs(WOS_DOWNLOAD_DIR, exist_ok=True)
    conn = get_papers_db()
    init_run_log_table(conn)
    conn.close()

    if args.command is None:
        parser.print_help()
        return

    if args.command == "cnki":
        asyncio.run(handle_cnki(args))
    elif args.command == "wos":
        asyncio.run(handle_wos(args))
    elif args.command == "batch":
        asyncio.run(handle_batch(args))
    elif args.command == "add-task":
        handle_add_task(args)
    elif args.command == "import-bib":
        # 委托给 bib_parser 模块 (迁移自智研TRACK bib转换db.py)
        from .bib_parser import main as bib_main
        # 重写 sys.argv 让 bib_parser 的 argparse 可以正确解析
        sys.argv = ["xiaoyan.scraper.bib_parser", args.path,
                    "--task-id", str(args.task_id)]
        if args.jcr:
            sys.argv.append("--jcr")
        bib_main()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
