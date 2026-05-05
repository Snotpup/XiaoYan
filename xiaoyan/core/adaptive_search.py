"""
xiaoyan.core.adaptive_search — 智慧检索引擎
================================================
目标导向的自适应文献检索闭环，最多 10 轮迭代。

每轮:
  1. 生成/优化检索式
  2. 调用 scraper 执行爬取
  3. 基于 {total_found, papers, title_relevance} 动态评估
  4. 飞书实时推送进展
  5. 决策: GOAL_MET → 推送结果 | REFINE → 继续 | ABORT → 推送最佳

核心设计:
  - "当前最佳" (best_round) 追踪: ABORT 时推送历史最优解
  - 循环检测: 连续相同检索式/结果数 → 自动终止
  - 双维度评估矩阵: 数量区间 × 标题相关率
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field

from xiaoyan.config import LARK_CHAT_ID
from xiaoyan.core.db import mark_paper_pushed
from xiaoyan.core.profile import ResearchProfile
from xiaoyan.core.intent import (
    refine_search_query,
    check_title_relevance,
    generate_paper_comment,
)
from xiaoyan.core.utils import looks_chinese, apply_date_range
from xiaoyan.lark.messenger import reply_text, send_card
from xiaoyan.lark.card_builder import build_paper_card

logger = logging.getLogger("xiaoyan.core.adaptive_search")


# ============================================================
#  数据结构
# ============================================================

@dataclass
class RoundResult:
    """单轮检索结果"""
    round_num: int
    query: str
    total_found: int = 0         # 平台报告总数
    papers: list = field(default_factory=list)  # 实际获取的论文
    relevance_rate: float = 0.0  # LLM 标题相关性 (0.0~1.0)
    decision: str = ""           # GOAL_MET / REFINE / ABORT
    reason: str = ""             # 人类可读的决策理由


# ============================================================
#  自适应检索器
# ============================================================

class AdaptiveSearcher:
    """
    目标导向自适应检索器。

    用法:
        searcher = AdaptiveSearcher(scheduler, message_id)
        await searcher.search(topic, query, platform, date_range)
    """

    MAX_ROUNDS = 10

    def __init__(self, scheduler, message_id: str, profile_summary: str = ""):
        self._scheduler = scheduler
        self._message_id = message_id
        self._profile_summary = profile_summary
        self._history: list[RoundResult] = []
        self._best_round: RoundResult | None = None
        self._topic: str = ""
        self._used_queries: set[str] = set()  # 探测检索式去重

    async def search(
        self,
        topic: str,
        query: str,
        platform: str,
        date_range: str = "recent_30d",
    ) -> None:
        """
        主入口 — 目标导向迭代搜索。

        Args:
            topic: 用户研究主题 (自然语言描述)
            query: LLM 生成的初始检索式
            platform: "wos" 或 "cnki"
            date_range: 时间范围
        """
        self._topic = topic

        # 平台自动修正: 检索式包含中文 → cnki
        if platform == "wos" and query and looks_chinese(query):
            platform = "cnki"
            if query.startswith("TS="):
                keywords = re.findall(r'[\u4e00-\u9fff]+[\w]*', query)
                if keywords:
                    query = "SU=('" + "' + '".join(keywords) + "')"
            logger.info("平台自动修正: wos → cnki (检索式包含中文)")

        # 时间范围约束
        query = apply_date_range(query, platform, date_range)

        # 开场飞书通知
        await reply_text(
            self._message_id,
            f"🔍 收到！正在为你智能检索「{topic}」({platform.upper()})\n"
            f"检索式: {query}\n"
            f"⏳ 最多 {self.MAX_ROUNDS} 轮自适应优化..."
        )

        current_query = query

        for round_num in range(1, self.MAX_ROUNDS + 1):
            logger.info(
                f"=== 第 {round_num}/{self.MAX_ROUNDS} 轮 === "
                f"query={current_query[:60]}"
            )

            # --- 执行爬取 ---
            try:
                scrape_result = await self._scheduler._run_scraper(
                    platform=platform,
                    query=current_query,
                    source_filters="",
                    skip_pushed_filter=True,
                )
            except asyncio.TimeoutError:
                await reply_text(
                    self._message_id,
                    f"⏱ 第 {round_num} 轮爬取超时，"
                    f"{platform.upper()} 可能需要验证码认证。\n"
                    f"请运行 `xiaoyan auth` 完成浏览器认证后重试。"
                )
                return
            except Exception as e:
                logger.error(f"第 {round_num} 轮爬取异常: {e}", exc_info=True)
                await reply_text(
                    self._message_id,
                    f"❌ 第 {round_num} 轮爬取出错: {str(e)[:150]}"
                )
                # 如果有历史最佳，降级推送
                if self._best_round and self._best_round.papers:
                    await self._push_final(platform, is_fallback=True)
                return

            # --- 构造本轮结果 ---
            result = RoundResult(
                round_num=round_num,
                query=current_query,
                total_found=scrape_result["total_found"],
                papers=scrape_result["papers"],
            )

            # --- 评估 ---
            decision, reason = await self._evaluate(result)
            result.decision = decision
            result.reason = reason
            self._history.append(result)

            # --- 飞书进度推送 ---
            await self._push_progress(result, platform)

            # --- 决策分发 ---
            if decision == "GOAL_MET":
                logger.info(f"GOAL_MET @ round {round_num}: {reason}")
                await self._push_final(platform, is_fallback=False)
                return

            elif decision == "ABORT":
                logger.info(f"ABORT @ round {round_num}: {reason}")
                if self._best_round and self._best_round.papers:
                    await self._push_final(platform, is_fallback=True)
                else:
                    await reply_text(
                        self._message_id,
                        f"📭 经过 {round_num} 轮尝试，未找到足够相关的文献。\n"
                        f"💡 建议: 尝试更换关键词描述或更换平台"
                    )
                return

            else:  # REFINE
                logger.info(f"REFINE @ round {round_num}: {reason}")

                # LLM 优化检索式
                refine_result = await refine_search_query(
                    original_query=current_query,
                    platform=platform,
                    round_result={
                        "total_found": result.total_found,
                        "papers_count": len(result.papers),
                        "sample_titles": [p["title"] for p in result.papers[:10]],
                        "failure_reason": reason,
                    },
                    topic=topic,
                )

                new_query = refine_result["query"]
                # 为新检索式也应用时间范围
                new_query = apply_date_range(new_query, platform, date_range)
                current_query = new_query

        # 已达最大轮次
        logger.warning(f"达到最大轮次 {self.MAX_ROUNDS}")
        if self._best_round and self._best_round.papers:
            await self._push_final(platform, is_fallback=True)
        else:
            await reply_text(
                self._message_id,
                f"📭 经过 {self.MAX_ROUNDS} 轮尝试仍未找到满意结果。\n"
                f"💡 建议: 手动调整检索关键词后重试"
            )

    # ================================================================
    #  评估函数
    # ================================================================

    async def _evaluate(self, result: RoundResult) -> tuple[str, str]:
        """
        基于双维度 (数量 × 质量) 评估本轮结果。

        Returns:
            (decision, reason)
            decision: "GOAL_MET" | "REFINE" | "ABORT"
        """
        total = result.total_found
        papers = result.papers
        papers_count = len(papers)

        # --- 特殊终止条件 ---
        if self._should_abort_loop(result):
            return ("ABORT", "检索策略已无优化空间 (连续相同结果)")

        # --- 零结果 ---
        if total == 0 and papers_count == 0:
            return ("REFINE", "零结果，需要放宽检索或更换术语")

        # --- 有论文时做相关性评估 ---
        if papers_count > 0:
            titles = [p["title"] for p in papers[:10]]
            relevance_list = await check_title_relevance(titles, self._topic)
            relevant_count = sum(1 for r in relevance_list if r)
            relevance_rate = relevant_count / len(relevance_list) if relevance_list else 0.0
            result.relevance_rate = relevance_rate

            # 更新 best_round
            self._update_best(result)

            # --- 综合判定 ---
            # 高质量: 相关率够且有足够论文
            if relevance_rate >= 0.5 and papers_count >= 3:
                return ("GOAL_MET", f"相关率 {relevance_rate:.0%} ({relevant_count}/{len(relevance_list)})，质量达标")

            # 数量合理 + 质量尚可
            if relevance_rate >= 0.3 and 1 <= total <= 500:
                return ("GOAL_MET", f"相关率 {relevance_rate:.0%}，数量合理 ({total} 篇)")

            # 结果过多
            if total > 5000:
                if relevance_rate >= 0.5:
                    return ("GOAL_MET", f"结果多 ({total}) 但相关率高 ({relevance_rate:.0%})")
                return ("REFINE", f"结果过多 ({total})，需加限定")

            # 结果偏多
            if total > 500:
                if relevance_rate >= 0.5:
                    return ("GOAL_MET", f"相关率 {relevance_rate:.0%}，质量达标")
                return ("REFINE", f"结果偏多 ({total}) 且相关率低 ({relevance_rate:.0%})")

            # 标题偏题
            if relevance_rate < 0.3:
                return ("REFINE", f"相关率仅 {relevance_rate:.0%}，标题偏题")

            # 兜底: 少量但相关
            return ("GOAL_MET", f"相关率 {relevance_rate:.0%}，通过")

        # --- 有 total 但 papers 为空 ---
        if total > 0 and papers_count == 0:
            return ("REFINE", f"平台报告 {total} 结果但未提取到论文")

        return ("GOAL_MET", "默认通过")

    def _should_abort_loop(self, current: RoundResult) -> bool:
        """检测是否应中止循环 (连续重复检测)"""
        if len(self._history) < 2:
            return False

        prev = self._history[-1]

        # 连续 2 轮使用相同检索式
        if current.query.strip() == prev.query.strip():
            return True

        # 连续 2 轮 total_found 完全相同且非零
        if len(self._history) >= 2:
            prev2 = self._history[-2]
            if (prev.total_found == current.total_found == prev2.total_found
                    and current.total_found > 0):
                return True

        return False

    def _update_best(self, result: RoundResult):
        """更新历史最佳 (相关率最高的轮次)"""
        if not self._best_round:
            self._best_round = result
        elif result.relevance_rate > self._best_round.relevance_rate:
            self._best_round = result
        elif (result.relevance_rate == self._best_round.relevance_rate
              and len(result.papers) > len(self._best_round.papers)):
            self._best_round = result

    # ================================================================
    #  飞书推送
    # ================================================================

    async def _push_progress(self, result: RoundResult, platform: str):
        """推送每轮检索进展到飞书"""
        status_icon = {
            "GOAL_MET": "✅",
            "REFINE": "🔄",
            "ABORT": "⛔",
        }.get(result.decision, "❓")

        lines = [
            f"{status_icon} 第 {result.round_num}/{self.MAX_ROUNDS} 轮检索",
            f"检索式: {result.query[:80]}",
            f"结果: {result.total_found} 篇 (平台总数)",
        ]

        if result.papers:
            lines.append(f"提取: {len(result.papers)} 篇")
            if result.relevance_rate > 0:
                lines.append(f"相关性: {result.relevance_rate:.0%}")

        lines.append(f"状态: {result.reason}")

        if result.decision == "REFINE":
            lines.append("→ 正在优化检索策略...")

        await reply_text(self._message_id, "\n".join(lines))

    async def _push_final(self, platform: str, is_fallback: bool):
        """推送最终结果到飞书"""
        source = self._best_round if is_fallback else self._history[-1]
        if not source or not source.papers:
            return

        papers = source.papers

        # P1-2: 画像过滤 (should_push 硬规则)
        profile = ResearchProfile()
        papers = [p for p in papers if profile.should_push(p)]
        if not papers:
            logger.info("画像过滤后无剩余论文，跳过推送")
            return

        # 为前 5 篇生成评述
        for paper in papers[:5]:
            comment = await generate_paper_comment(
                paper["title"], paper["abstract"],
                profile_summary=self._profile_summary,
            )
            paper["llm_comment"] = comment

        # 逐条推送卡片 (最多 5 篇)
        for paper in papers[:5]:
            card_json = build_paper_card(
                title=paper["title"],
                journal=paper["journal"],
                abstract=paper["abstract"],
                llm_comment=paper["llm_comment"],
                platform=platform,
                url=paper.get("url", ""),
            )
            await send_card(LARK_CHAT_ID, card_json)
            mark_paper_pushed(paper["data_hash"], platform, 0)
            await asyncio.sleep(0.3)

        # 总结消息
        total_rounds = len(self._history)
        final_query = source.query

        fallback_note = ""
        if is_fallback:
            fallback_note = f"\n📌 使用第 {source.round_num} 轮的最佳结果"

        # 收集关键调整历史
        adjustments = []
        for i, h in enumerate(self._history):
            if i > 0 and h.query != self._history[i-1].query:
                adjustments.append(
                    f"  轮 {h.round_num}: {self._history[i-1].query[:30]}... "
                    f"→ {h.query[:30]}..."
                )

        summary_lines = [
            f"✅ 智能检索完成！",
            f"轮次: {total_rounds}/{self.MAX_ROUNDS}",
            f"最终检索式: {final_query[:60]}",
            f"找到 {source.total_found} 篇，推送 Top {min(5, len(papers))}",
        ]

        if adjustments:
            summary_lines.append("关键调整:")
            summary_lines.extend(adjustments[:3])

        if fallback_note:
            summary_lines.append(fallback_note)

        await reply_text(self._message_id, "\n".join(summary_lines))

        # P1-1: 同步到飞书 Base (失败不阻塞推送)
        try:
            import os
            from xiaoyan.lark.base_sync import get_base_manager
            base_mgr = get_base_manager()
            if base_mgr.is_configured or os.environ.get("XIAOYAN_BASE_TOKEN"):
                for paper in papers:
                    paper["platform"] = platform
                await base_mgr.sync_papers(papers)
        except Exception as e:
            logger.warning(f"Base 同步失败 (不影响推送): {e}")

    # ================================================================
    #  深度探索模式
    #  迁移自智研TRACK 知网文献检索.py 的三阶段 LLM 驱动检索策略:
    #  1. 概念分解 → 2. 多维度探测 → 3. 最终策略报告
    #  增量改进: 双平台 (CNKI + WoS) 支持
    # ================================================================

    MAX_EXPLORE_ROUNDS = 10  # 深度探索最大轮次 (源项目用 50, 加收敛检测后无需更多)

    async def deep_explore(
        self,
        topic: str,
        platform: str = "wos",
    ) -> None:
        """
        深度探索模式 — LLM 驱动的多维度文献探测。

        与 search() 的区别:
        - search(): 单检索式迭代优化, 目标是找到当前最好的一批论文
        - deep_explore(): 多维度探测, 目标是生成一个全面的检索策略报告

        Args:
            topic: 用户研究课题 (自然语言描述)
            platform: "wos", "cnki", 或 "both" (双平台)
        """
        from xiaoyan.core.intent import decompose_topic, decide_next_explore_step

        self._topic = topic

        # --- Phase 1: 课题分解 ---
        await reply_text(
            self._message_id,
            f"🧠 正在分析研究课题: 「{topic}」\n"
            f"📋 阶段 1/3: LLM 概念分解中..."
        )

        decomp = await decompose_topic(topic)
        concepts = decomp.get("core_concepts", [])
        probes = decomp.get("initial_probes", [])

        if not probes:
            await reply_text(
                self._message_id,
                "❌ 课题分解失败，未能生成探测检索式。\n"
                "💡 建议: 请尝试换一种方式描述你的研究主题"
            )
            return

        # 推送概念分解结果
        concept_lines = ["🔬 课题核心概念:"]
        for c in concepts:
            concept_lines.append(f"  • {c['concept']} ({c.get('role', '')})")
        concept_lines.append(f"\n📝 初始探测计划: {len(probes)} 个方向")
        await reply_text(self._message_id, "\n".join(concept_lines))

        # --- Phase 2: 多维度探测 ---
        await reply_text(
            self._message_id,
            "🔍 阶段 2/3: 多维度文献探测中...\n"
            f"⏳ 最多 {self.MAX_EXPLORE_ROUNDS} 轮探测"
        )

        probe_history = []

        # 执行初始探测
        for probe in probes:
            query = probe.get(f"{platform}_query") or probe.get("wos_query") or probe.get("cnki_query", "")
            if not query:
                continue

            # 格式守卫 + 去重
            if not self._validate_query_format(query, platform):
                logger.warning(f"初始探测 {probe['probe_id']} 跳过: 格式不匹配平台 {platform}")
                continue
            if self._is_duplicate_query(query):
                logger.info(f"初始探测 {probe['probe_id']} 跳过: 检索式已用过")
                continue

            probe_result = await self._run_single_probe(
                probe_id=probe["probe_id"],
                query=query,
                platform=platform,
            )
            probe_history.append(probe_result)

        # 迭代探测循环
        finalized_strategy = None

        for round_num in range(len(probes) + 1, self.MAX_EXPLORE_ROUNDS + 1):
            logger.info(f"=== 探测轮 {round_num}/{self.MAX_EXPLORE_ROUNDS} ===")

            # 代码级收敛检测: 连续 3 轮结果稳定 → 强制 finalize
            if self._should_finalize_exploration(probe_history):
                logger.info("收敛检测触发: 连续多轮结果稳定，强制 finalize")
                await reply_text(
                    self._message_id,
                    "📊 检测到探测结果已收敛，进入总结阶段..."
                )
                break

            # LLM 决策: 继续还是总结
            decision = await decide_next_explore_step(topic, probe_history, platform)

            if decision["decision"] == "finalize":
                logger.info(f"LLM 探测决策: finalize - {decision['analysis'][:60]}")
                finalized_strategy = decision.get("final_strategy", {})
                break

            # 继续探测
            next_probe = decision.get("next_probe", {})
            query = next_probe.get(f"{platform}_query") or next_probe.get("wos_query") or next_probe.get("cnki_query", "")
            if not query:
                logger.warning("LLM 决定继续但未提供检索式，终止探测")
                break

            # 格式守卫
            if not self._validate_query_format(query, platform):
                logger.warning(f"LLM 生成的检索式格式不匹配 {platform}, 跳过: {query[:50]}")
                await reply_text(
                    self._message_id,
                    f"⚠ 检索式格式不匹配 {platform.upper()}，已自动跳过"
                )
                continue

            # 去重
            if self._is_duplicate_query(query):
                logger.info(f"检索式已用过，跳过: {query[:50]}")
                continue

            probe_result = await self._run_single_probe(
                probe_id=next_probe.get("probe_id", f"P_{round_num:03d}"),
                query=query,
                platform=platform,
            )
            probe_history.append(probe_result)

        # --- Phase 3: 生成最终策略报告 ---
        await reply_text(
            self._message_id,
            "📊 阶段 3/3: 生成最终检索策略报告..."
        )

        # 仅在 Phase 2 未产生 finalize 策略时，才再次调用 LLM
        if finalized_strategy is None:
            logger.info("Phase 2 未产生 finalize 策略，强制请求最终策略...")
            final = await decide_next_explore_step(topic, probe_history, platform)
            finalized_strategy = final.get("final_strategy", {})

        strategy = finalized_strategy

        # 推送最终报告
        await self._push_explore_report(
            topic=topic,
            platform=platform,
            probe_history=probe_history,
            strategy=strategy,
        )

    # ================================================================
    #  深度探索辅助方法
    # ================================================================

    def _validate_query_format(self, query: str, platform: str) -> bool:
        """
        验证检索式格式是否匹配目标平台。

        CNKI 拒绝: TS=, AND, OR (WoS 格式)
        WoS 拒绝: SU=, TI=, KY=, *, + (CNKI 格式)
        """
        q = query.strip()
        if platform == "cnki":
            # CNKI 不应包含 WoS 格式标志
            if q.startswith("TS=") or q.startswith("TI=") and " AND " in q:
                return False
            if ' AND ' in q or ' OR ' in q:
                # 额外检查: 纯英文 + AND/OR → 很可能是 WoS 格式
                non_ascii = sum(1 for c in q if ord(c) > 127)
                if non_ascii == 0:  # 全英文
                    return False
        elif platform == "wos":
            if q.startswith("SU=") or q.startswith("KY="):
                return False
        return True

    def _is_duplicate_query(self, query: str) -> bool:
        """
        检查检索式是否已经用过（归一化后去重）。
        如果未用过，将其加入已用集合。
        """
        normalized = query.strip().lower()
        if normalized in self._used_queries:
            return True
        self._used_queries.add(normalized)
        return False

    def _should_finalize_exploration(self, probe_history: list[dict]) -> bool:
        """
        检测探测是否已收敛（连续 3 轮结果数量变化 ≤ 20%）。

        条件:
        - 至少已完成 5 轮探测
        - 最近 3 轮的 total_found 变化幅度都很小
        """
        if len(probe_history) < 5:
            return False

        recent = probe_history[-3:]
        totals = [h["total_found"] for h in recent]

        # 如果最近 3 轮全是 0，也应该终止
        if all(t == 0 for t in totals):
            return True

        # 计算变化幅度
        max_val = max(totals)
        min_val = min(totals)
        if max_val == 0:
            return True

        variation = (max_val - min_val) / max_val
        if variation <= 0.2:
            logger.info(
                f"收敛检测: 最近 3 轮 total_found = {totals}, "
                f"变化幅度 {variation:.0%} ≤ 20%"
            )
            return True

        return False

    async def _run_single_probe(
        self, probe_id: str, query: str, platform: str,
    ) -> dict:
        """执行单次探测爬取并返回结果摘要"""
        logger.info(f"探测 {probe_id}: [{platform}] {query[:60]}")

        await reply_text(
            self._message_id,
            f"🔎 {probe_id}: {query[:60]}..."
        )

        try:
            scrape_result = await self._scheduler._run_scraper(
                platform=platform,
                query=query,
                source_filters="",
                skip_pushed_filter=True,
            )
            papers = scrape_result["papers"]
            total = scrape_result["total_found"]

            result = {
                "probe_id": probe_id,
                "query": query,
                "platform": platform,
                "total_found": total,
                "papers_count": len(papers),
                "sample_titles": [p["title"] for p in papers[:10]],
            }

            await reply_text(
                self._message_id,
                f"  → {probe_id}: {total} 篇 (提取 {len(papers)} 篇)"
            )

            return result

        except Exception as e:
            logger.error(f"探测 {probe_id} 失败: {e}")
            return {
                "probe_id": probe_id,
                "query": query,
                "platform": platform,
                "total_found": 0,
                "papers_count": 0,
                "sample_titles": [],
                "error": str(e),
            }

    async def _push_explore_report(
        self,
        topic: str,
        platform: str,
        probe_history: list[dict],
        strategy: dict,
    ):
        """推送深度探索最终报告到飞书 (P1-3: 飞书文档输出)"""
        # 组装 Markdown 报告
        lines = [
            f"# 📖 深度文献探索报告",
            f"",
            f"**课题**: {topic}",
            f"**平台**: {platform.upper()}",
            f"**探测轮次**: {len(probe_history)}",
            "",
            "---",
            "",
            "## 🔍 探测结果",
            "",
        ]

        for h in probe_history:
            status = "✅" if h["total_found"] > 0 else "❌"
            lines.append(
                f"- {status} **{h['probe_id']}**: "
                f"{h['total_found']} 篇 | `{h['query'][:60]}`"
            )
            # 展示样本标题
            titles = h.get("sample_titles", [])[:3]
            for t in titles:
                lines.append(f"  - {t}")

        # 最终策略
        if strategy:
            lines.append("")
            lines.append("## 📋 推荐检索策略")
            lines.append("")
            summary = strategy.get("summary", "")
            if summary:
                lines.append(f"{summary}")
                lines.append("")

            queries = strategy.get("recommended_queries", [])
            for q in queries:
                lines.append(f"### 【{q.get('category', '')}】")
                if q.get("cnki_query"):
                    lines.append(f"- **CNKI**: `{q['cnki_query']}`")
                if q.get("wos_query"):
                    lines.append(f"- **WoS**: `{q['wos_query']}`")
                if q.get("expected_papers"):
                    lines.append(f"- 预期范围: {q['expected_papers']}")
                lines.append("")

            advice = strategy.get("advice", [])
            if advice:
                lines.append("## 💡 建议")
                lines.append("")
                for a in advice:
                    lines.append(f"- {a}")

        report_md = "\n".join(lines)

        # P1-3: 优先创建飞书文档
        try:
            from xiaoyan.lark.doc_writer import LarkDocWriter
            from datetime import datetime

            writer = LarkDocWriter()
            title = f"深度探索报告 — {topic[:20]} ({datetime.now().strftime('%m-%d')})"
            result = await writer.create_doc(
                title=title,
                markdown=report_md,
                wiki_space="my_library",
            )
            doc_url = result.get("url", "")

            summary_msg = (
                f"📖 **深度探索报告已生成！**\n\n"
                f"📄 [点击查看完整报告]({doc_url})\n\n"
                f"课题: {topic}\n"
                f"平台: {platform.upper()} | 探测轮次: {len(probe_history)}"
            )
            from xiaoyan.lark.messenger import reply_markdown
            await reply_markdown(self._message_id, summary_msg)
            logger.info(f"深度探索报告已创建飞书文档: {doc_url}")

        except Exception as e:
            logger.warning(f"飞书文档创建失败，降级为 IM 推送: {e}")
            # 降级: 纯文本推送 (截断)
            fallback = report_md[:2000]
            if len(report_md) > 2000:
                fallback += "\n\n---\n⚠ 报告较长，但飞书文档创建失败，已截断显示"
            await reply_text(self._message_id, fallback)
