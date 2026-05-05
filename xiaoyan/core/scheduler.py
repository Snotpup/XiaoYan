"""
xiaoyan.core.scheduler — 定时爬取调度器
==========================================
基于 APScheduler 实现 cron 定时触发爬取任务。

职责:
  - 从 SQLite 读取活跃检索式
  - 为每个检索式注册 cron job
  - 爬取完成后触发推送流程
  - 支持动态增删检索式 (无需重启)
  - 爬取失败自动指数退避重试 (P1-4)
  - 每日定时推送爬取摘要 (P1-4, 时间可配置)
"""

import asyncio
import logging
import os
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from xiaoyan.core.db import list_search_queries, is_paper_pushed, mark_paper_pushed, get_setting_int, get_setting
from xiaoyan.core.intent import generate_paper_comment
from xiaoyan.lark.messenger import send_card
from xiaoyan.lark.card_builder import (
    build_paper_card, build_papers_batch_card, build_daily_summary_card,
)
from xiaoyan.config import (
    LARK_CHAT_ID, WOS_MAX_RECORDS, WOS_SITE, CNKI_MAX_PAGES,
)

logger = logging.getLogger("xiaoyan.core.scheduler")

# ── P1-4: 重试策略常量 ──
RETRY_MAX_ATTEMPTS = 3
RETRY_BASE_DELAY = 30  # 秒


class ScrapeScheduler:
    """
    定时爬取调度器。

    用法:
        scheduler = ScrapeScheduler()
        await scheduler.start()   # 加载所有检索式并启动调度
        await scheduler.reload()  # 动态重载检索式
        scheduler.shutdown()      # 停止调度
    """

    def __init__(self):
        self._scheduler = AsyncIOScheduler(
            job_defaults={"coalesce": True, "max_instances": 1}
        )
        # 认证失效检测: 连续失败计数 + 通知冷却
        self._consecutive_auth_failures = 0
        self._last_auth_notify_time = None

    @staticmethod
    def _get_daily_summary_trigger() -> CronTrigger:
        """从数据库读取每日摘要时间并创建 CronTrigger"""
        time_str = get_setting("daily_summary_time") or "21:00"
        try:
            parts = time_str.split(":")
            hour = int(parts[0])
            minute = int(parts[1]) if len(parts) > 1 else 0
            return CronTrigger(hour=hour, minute=minute)
        except (ValueError, IndexError):
            logger.warning(f"每日摘要时间格式无效: {time_str}，使用默认 21:00")
            return CronTrigger(hour=21, minute=0)

    async def start(self):
        """加载所有活跃检索式，注册 cron job 并启动调度器"""
        await self._load_jobs()

        # P1-4: 注册每日摘要推送
        trigger = self._get_daily_summary_trigger()
        self._scheduler.add_job(
            self._push_daily_summary,
            trigger,
            id="daily_summary",
            replace_existing=True,
        )

        self._scheduler.start()
        logger.info("定时调度器已启动")

    async def reload(self):
        """重载所有检索式 (增删改后调用)"""
        # 移除所有现有 job
        self._scheduler.remove_all_jobs()
        await self._load_jobs()

        # 重新注册每日摘要 (reload 会清掉所有 job，包括 daily_summary)
        trigger = self._get_daily_summary_trigger()
        self._scheduler.add_job(
            self._push_daily_summary,
            trigger,
            id="daily_summary",
            replace_existing=True,
        )

        logger.info("检索式已重载 (含每日摘要)")

    async def _load_jobs(self):
        """从数据库加载活跃检索式并注册 cron job"""
        queries = list_search_queries(active_only=True)
        for q in queries:
            cron_expr = q.get("cron_expression", "0 */6 * * *")
            try:
                trigger = CronTrigger.from_crontab(cron_expr)
            except ValueError as e:
                logger.error(f"检索式 #{q['id']} cron 表达式无效: {cron_expr} ({e})")
                continue

            self._scheduler.add_job(
                self._execute_scrape,
                trigger=trigger,
                id=f"scrape_query_{q['id']}",
                args=[q],
                replace_existing=True,
            )
            logger.info(
                f"已注册定时任务: #{q['id']} [{q['platform']}] "
                f"cron={cron_expr} | {q['query'][:50]}"
            )

    async def _execute_scrape(self, query: dict):
        """执行单个检索式的爬取 + 推送 (带指数退避重试, P1-4)"""
        query_id = query["id"]
        platform = query["platform"]
        search_query = query["query"]
        source_filters = query.get("source_filters", "")

        logger.info(f"⏰ 定时触发爬取: #{query_id} [{platform}] {search_query[:50]}")

        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            try:
                result = await self._run_scraper(
                    platform=platform,
                    query=search_query,
                    source_filters=source_filters,
                )
                new_papers = result["papers"]

                if new_papers:
                    await self._push_new_papers(
                        papers=new_papers,
                        platform=platform,
                        query_id=query_id,
                        query_desc=query.get("description", ""),
                    )
                    logger.info(f"✅ 爬取完成: #{query_id} 新增 {len(new_papers)} 篇")
                else:
                    logger.info(f"爬取完成: #{query_id} 无新增文献")

                # 成功: 重置连续失败计数并退出重试循环
                self._consecutive_auth_failures = 0
                return

            except Exception as e:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                if attempt < RETRY_MAX_ATTEMPTS:
                    logger.warning(
                        f"爬取任务 #{query_id} 第 {attempt}/{RETRY_MAX_ATTEMPTS} 次失败: {e}"
                        f" → {delay}s 后重试"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"爬取任务 #{query_id} 第 {attempt}/{RETRY_MAX_ATTEMPTS} 次失败: {e}"
                        f" → 已达重试上限",
                        exc_info=True,
                    )
                    # 最终失败: 认证检测
                    await self._check_auth_failure(platform, query_id, str(e))

    # ── 认证失效检测 + 飞书通知 ──

    # 认证失败关键词 (scraper 输出或 run_logs status)
    _AUTH_FAIL_KEYWORDS = {
        "verification", "verify", "验证", "captcha", "cloudflare",
        "滑块", "拼图", "challenge", "blocked", "403",
        "无头模式下无法完成验证",
    }

    async def _check_auth_failure(
        self, platform: str, query_id: int, error_msg: str
    ):
        """
        检测爬取失败是否由认证/验证问题导致。
        如果连续 2 次以上判定为认证失败，通过飞书通知用户修复。
        """
        # 检查错误消息中是否包含认证相关关键词
        error_lower = error_msg.lower()
        is_auth_issue = any(
            kw in error_lower for kw in self._AUTH_FAIL_KEYWORDS
        )

        if is_auth_issue:
            self._consecutive_auth_failures += 1
            logger.warning(
                f"疑似认证失效 (连续第 {self._consecutive_auth_failures} 次): "
                f"{platform} #{query_id}"
            )

            # 连续失败 2 次以上才通知，避免误判
            if self._consecutive_auth_failures >= 2:
                await self._notify_auth_required(platform)
        else:
            # 非认证问题的失败，不增加计数
            pass

    async def _notify_auth_required(self, platform: str):
        """
        通过飞书通知用户: 学术平台会话可能已过期，需要重新认证。
        有 6 小时冷却期，避免频繁打扰。
        """
        now = datetime.now()

        # 冷却检查: 6 小时内不重复通知
        if self._last_auth_notify_time:
            elapsed = (now - self._last_auth_notify_time).total_seconds()
            if elapsed < 6 * 3600:
                logger.debug(
                    f"认证通知冷却中 (距上次 {elapsed/3600:.1f}h，需 6h)"
                )
                return

        self._last_auth_notify_time = now

        platform_name = {"cnki": "知网", "wos": "WoS"}.get(platform, platform)

        message = (
            f"⚠️ {platform_name} 会话可能已过期\n\n"
            f"小研在执行定时爬取时，连续遇到验证失败。\n"
            f"这通常是因为浏览器 Cookie 过期或网络环境变化。\n\n"
            f"🔧 请在电脑上运行以下命令修复:\n"
            f"   xiaoyan auth\n\n"
            f"📍 请确保在校园网/机构 VPN 下操作。\n"
            f"完成后，定时任务将自动恢复正常。"
        )

        try:
            from xiaoyan.lark.messenger import send_text
            await send_text(LARK_CHAT_ID, message)
            logger.info(f"已发送认证过期通知到飞书: {platform_name}")
        except Exception as e:
            logger.error(f"发送认证过期通知失败: {e}")

    async def _run_scraper(
        self, platform: str, query: str, source_filters: str = "",
        skip_pushed_filter: bool = False,
    ) -> dict:
        """
        调用 Scraper 执行爬取，返回结果字典。

        Returns:
            {
                "papers": [{"title": ..., "journal": ..., ...}, ...],
                "total_found": int,   # 平台报告的总结果数
                "task_id": int,
            }
        """
        from xiaoyan.scraper.db_manager import (
            get_papers_db, get_tasks_db, init_tasks_table,
            add_task, init_cnki_table, init_wos_quick_table,
            init_run_log_table,
        )
        from xiaoyan.scraper.config import SCRAPE_GLOBAL_TIMEOUT

        # 创建 scraper 任务记录
        tasks_conn = get_tasks_db()
        init_tasks_table(tasks_conn)
        sources = source_filters.split(",") if source_filters else []
        task_id = add_task(
            tasks_conn, platform, query,
            source_key=",".join(sources),
            source_name=",".join(sources),
        )
        tasks_conn.close()

        # 执行爬取 (带全局超时保护，参数从 db 动态读取)
        cnki_pages = get_setting_int("cnki_max_pages", CNKI_MAX_PAGES)
        wos_records = get_setting_int("wos_max_records", WOS_MAX_RECORDS)

        try:
            if platform == "cnki":
                from xiaoyan.scraper import scrape_cnki
                await asyncio.wait_for(
                    scrape_cnki(
                        task_id=task_id,
                        query=query,
                        source_filters=sources,
                        show_browser=False,
                        max_pages=cnki_pages,
                    ),
                    timeout=SCRAPE_GLOBAL_TIMEOUT,
                )
            elif platform == "wos":
                from xiaoyan.scraper import scrape_wos
                await asyncio.wait_for(
                    scrape_wos(
                        task_id=task_id,
                        query=query,
                        mode="quick",
                        max_records=wos_records,
                        show_browser=False,
                        site=WOS_SITE,
                    ),
                    timeout=SCRAPE_GLOBAL_TIMEOUT,
                )
            else:
                logger.error(f"未知平台: {platform}")
                return {"papers": [], "total_found": 0, "task_id": task_id}
        except asyncio.TimeoutError:
            logger.error(
                f"爬取超时 ({SCRAPE_GLOBAL_TIMEOUT}秒)，已强制终止: "
                f"{platform} | {query[:50]}"
            )
            raise  # 向上传播，让 engine 层处理超时提示

        # 从 scraper 数据库读取新入库的论文
        papers_conn = get_papers_db()
        init_run_log_table(papers_conn)

        new_papers = []
        total_found = 0
        try:
            # 读取 total_found (scraper 在爬取时已写入 run_logs)
            try:
                row = papers_conn.execute(
                    "SELECT total_found FROM run_logs "
                    "WHERE task_id = ? ORDER BY id DESC LIMIT 1",
                    (task_id,)
                ).fetchone()
                if row:
                    total_found = row[0] or 0
            except Exception:
                pass

            if platform == "cnki":
                table_name = f"cnki_task_{task_id}"
                init_cnki_table(papers_conn, table_name)
                cursor = papers_conn.execute(
                    f'SELECT title, journal, abstract, data_hash, detail_url '
                    f'FROM "{table_name}" ORDER BY id'
                )
            else:
                table_name = f"wos_quick_{task_id}"
                init_wos_quick_table(papers_conn, table_name)
                cursor = papers_conn.execute(
                    f'SELECT title, journal, abstract, data_hash '
                    f'FROM "{table_name}" ORDER BY id'
                )

            rows = cursor.fetchall()
            logger.info(f"从 {table_name} 读取到 {len(rows)} 条记录")

            for row in rows:
                paper = {
                    "title": row[0] or "",
                    "journal": row[1] or "",
                    "abstract": row[2] or "",
                    "data_hash": row[3] or "",
                }
                if platform == "cnki" and len(row) > 4:
                    paper["url"] = row[4] or ""

                # 定时推送模式: 过滤已推送过的
                # 临时检索模式: 返回全部入库论文
                if skip_pushed_filter or not is_paper_pushed(paper["data_hash"]):
                    new_papers.append(paper)

            logger.info(
                f"返回 {len(new_papers)} 篇论文 "
                f"(total_found={total_found}, "
                f"skip_pushed_filter={skip_pushed_filter})"
            )
        finally:
            papers_conn.close()

        return {
            "papers": new_papers,
            "total_found": total_found,
            "task_id": task_id,
        }

    async def _push_new_papers(
        self,
        papers: list[dict],
        platform: str,
        query_id: int,
        query_desc: str = "",
    ):
        """为新文献生成 LLM 评述并推送到飞书"""
        if not LARK_CHAT_ID:
            logger.warning("LARK_CHAT_ID 未配置，跳过推送")
            return

        # P1-2: 画像过滤 (should_push 硬规则)
        from xiaoyan.core.profile import ResearchProfile
        profile = ResearchProfile()
        filtered = [p for p in papers if profile.should_push(p)]
        skipped = len(papers) - len(filtered)
        if skipped > 0:
            logger.info(f"画像过滤: 跳过 {skipped} 篇 (综述等硬规则)")
        if not filtered:
            logger.info("画像过滤后无剩余论文，跳过推送")
            return
        papers = filtered

        # 为每篇论文生成 LLM 评述 (带画像上下文)
        profile_summary = profile.format_summary()
        for paper in papers:
            comment = await generate_paper_comment(
                paper["title"], paper["abstract"],
                profile_summary=profile_summary,
            )
            paper["llm_comment"] = comment

        # 推送策略: 1-3 篇逐条推送, 4+ 篇用汇总卡片
        if len(papers) <= 3:
            for paper in papers:
                card_json = build_paper_card(
                    title=paper["title"],
                    journal=paper["journal"],
                    abstract=paper["abstract"],
                    llm_comment=paper["llm_comment"],
                    platform=platform,
                    url=paper.get("url", ""),
                )
                await send_card(LARK_CHAT_ID, card_json)
                mark_paper_pushed(paper["data_hash"], platform, query_id)
                await asyncio.sleep(0.5)  # 避免消息发送过快
        else:
            card_json = build_papers_batch_card(
                papers=papers,
                query_desc=query_desc,
                total_new=len(papers),
            )
            await send_card(LARK_CHAT_ID, card_json)
            for paper in papers:
                mark_paper_pushed(paper["data_hash"], platform, query_id)

        # P1-1: 同步到飞书 Base (失败不阻塞推送主流程)
        try:
            from xiaoyan.lark.base_sync import get_base_manager
            base_mgr = get_base_manager()
            if base_mgr.is_configured or os.environ.get("XIAOYAN_BASE_TOKEN"):
                for paper in papers:
                    paper["platform"] = platform
                await base_mgr.sync_papers(papers)
        except Exception as e:
            logger.warning(f"Base 同步失败 (不影响推送): {e}")

    async def _push_daily_summary(self):
        """推送过去 24h 的爬取摘要 — 直接从 run_logs 表聚合 (P1-4)"""
        from xiaoyan.scraper.db_manager import get_papers_db, init_run_log_table

        if not LARK_CHAT_ID:
            logger.warning("LARK_CHAT_ID 未配置，跳过每日摘要")
            return

        conn = get_papers_db()
        init_run_log_table(conn)  # 幂等

        try:
            rows = conn.execute("""
                SELECT
                    COUNT(*) as total_runs,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_runs,
                    SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END) as failed_runs,
                    SUM(new_added) as total_new
                FROM run_logs
                WHERE run_at >= datetime('now', 'localtime', '-24 hours')
            """).fetchone()

            if not rows or rows[0] == 0:
                logger.info("过去 24h 无任何执行记录，跳过每日摘要")
                return

            total_runs, success_runs, failed_runs, total_new = rows
            total_new = total_new or 0

            card_json = build_daily_summary_card(
                total_runs=total_runs,
                success_runs=success_runs,
                failed_runs=failed_runs,
                total_new_papers=total_new,
            )
            await send_card(LARK_CHAT_ID, card_json)
            logger.info(
                f"每日摘要已推送: {total_runs} 次执行, "
                f"{success_runs} 成功, {failed_runs} 失败, "
                f"新增 {total_new} 篇"
            )
        except Exception as e:
            logger.error(f"每日摘要推送失败: {e}", exc_info=True)
        finally:
            conn.close()

    def shutdown(self):
        """停止调度器"""
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("定时调度器已停止")

    async def run_now(self, query_id: int) -> int:
        """
        立即执行指定检索式的爬取 (用于临时触发)。

        Returns:
            新增论文数
        """
        from xiaoyan.core.db import get_search_query
        query = get_search_query(query_id)
        if not query:
            logger.error(f"检索式 #{query_id} 不存在")
            return 0
        await self._execute_scrape(query)
        return 1  # TODO: 返回实际新增数
