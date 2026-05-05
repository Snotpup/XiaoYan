"""
xiaoyan.core.report — 文献分析报告生成 (P1-3)
================================================
从 SQLite 读取文献 → LLM 生成结构化 Markdown 报告。

核心难点:
  - 论文数据碎片化: cnki_task_X / wos_quick_X / wos_bibtex_X 三种表结构
  - 需归一化为统一的 {title, journal, abstract} 字典

报告类型:
  - summary (批次摘要): 每篇一句话 + 整体概况
  - review  (综述草稿): 主题归类 + 各类别核心发现
  - trend   (趋势分析): 热门关键词 + 方法论演变

参考: [P1-3-report-generation.md]
"""

import logging

from openai import AsyncOpenAI

from xiaoyan.config import LLM_API_KEY, LLM_BASE_URL, LLM_MODEL

logger = logging.getLogger("xiaoyan.core.report")


# ============================================================
#  LLM 报告 System Prompt
# ============================================================

REPORT_GENERATION_PROMPT = """\
你是学术综述写作助手。根据以下文献列表，生成一份结构化的研究分析报告。

## 输入格式
每篇论文以 [编号] 标识，包含标题、期刊、摘要。

## 输出要求
1. **整体概况** (2-3 句话总结这批文献的研究方向)
2. **主题归类** (将文献分为 2-4 个主题类别，每类列出对应文献编号)
3. **核心发现** (每个主题类别的 2-3 个关键研究发现)
4. **研究趋势** (是否出现新方法/新数据/新视角)
5. **建议关注** (推荐深入阅读的 Top 3 论文及理由)

## 格式
输出纯 Markdown，使用中文。必须包含 ## 级标题以便飞书文档生成目录。
"""

TREND_REPORT_PROMPT = """\
你是学术趋势分析专家。根据以下文献列表，分析研究热点和方法论趋势。

## 输出要求
1. **热门关键词** (出现频率最高的研究主题)
2. **方法论演变** (主流研究方法和数据来源)
3. **新兴方向** (出现的新视角/新框架)
4. **研究空白** (可能被忽视的方向)

## 格式
输出纯 Markdown，使用中文。必须包含 ## 级标题。
"""

SUMMARY_REPORT_PROMPT = """\
你是学术文献摘要助手。为以下文献生成简明的批次摘要。

## 输出要求
1. **整体概况** (2-3 句话)
2. **逐篇摘要** (每篇一句话总结，标注编号)
3. **亮点推荐** (最值得关注的 1-3 篇及理由)

## 格式
输出纯 Markdown，使用中文。必须包含 ## 级标题。
"""


class ReportGenerator:
    """文献分析报告生成器"""

    async def fetch_papers(self, source: str = "recent") -> list[dict]:
        """
        从 SQLite 获取文献列表，归一化不同表结构。

        Args:
            source:
              "recent"  → 最近一次成功且有新增的 run_log
              "today"   → 今天所有成功 run_logs（跨表去重）

        Returns:
            [{"title": str, "journal": str, "abstract": str}, ...]
        """
        from xiaoyan.scraper.db_manager import (
            get_papers_db, init_run_log_table,
            init_cnki_table, init_wos_quick_table,
        )

        conn = get_papers_db()
        init_run_log_table(conn)

        try:
            if source == "recent":
                row = conn.execute(
                    "SELECT task_id, platform FROM run_logs "
                    "WHERE status = 'success' AND new_added > 0 "
                    "ORDER BY id DESC LIMIT 1"
                ).fetchone()
                if not row:
                    return []
                return self._read_papers_from_table(conn, row[0], row[1])

            elif source == "today":
                rows = conn.execute(
                    "SELECT task_id, platform FROM run_logs "
                    "WHERE status = 'success' AND new_added > 0 "
                    "AND date(run_at) = date('now', 'localtime') ORDER BY id"
                ).fetchall()
                seen, papers = set(), []
                for task_id, platform in rows:
                    for p in self._read_papers_from_table(conn, task_id, platform):
                        h = p.get("data_hash", p["title"])
                        if h not in seen:
                            seen.add(h)
                            papers.append(p)
                return papers

            else:
                logger.warning(f"未知的 source: {source}，使用 recent")
                return await self.fetch_papers("recent")

        finally:
            conn.close()

    def _read_papers_from_table(
        self, conn, task_id: int, platform: str
    ) -> list[dict]:
        """从指定任务表读取论文并归一化"""
        from xiaoyan.scraper.db_manager import (
            init_cnki_table, init_wos_quick_table,
        )

        papers = []
        try:
            if platform == "cnki":
                table_name = f"cnki_task_{task_id}"
                init_cnki_table(conn, table_name)
                cursor = conn.execute(
                    f'SELECT title, journal, abstract, data_hash '
                    f'FROM "{table_name}" ORDER BY id'
                )
            elif platform == "wos":
                table_name = f"wos_quick_{task_id}"
                init_wos_quick_table(conn, table_name)
                cursor = conn.execute(
                    f'SELECT title, journal, abstract, data_hash '
                    f'FROM "{table_name}" ORDER BY id'
                )
            else:
                return []

            for row in cursor.fetchall():
                papers.append({
                    "title": row[0] or "",
                    "journal": row[1] or "",
                    "abstract": row[2] or "",
                    "data_hash": row[3] or "",
                })
        except Exception as e:
            logger.warning(f"读取 {platform} task {task_id} 失败: {e}")

        return papers

    async def generate(
        self,
        papers: list[dict],
        report_type: str = "summary",
        topic: str = "",
    ) -> str:
        """
        调用 LLM 生成文献分析报告。

        Args:
            papers: 归一化后的论文列表
            report_type: "summary" | "review" | "trend"
            topic: 可选的主题过滤描述

        Returns:
            Markdown 格式报告内容
        """
        if not papers:
            return "## 无可分析的文献\n\n请先执行一次检索以获取文献数据。"

        if not LLM_API_KEY:
            return self._fallback_report(papers)

        # 选择 system prompt
        prompt_map = {
            "summary": SUMMARY_REPORT_PROMPT,
            "review": REPORT_GENERATION_PROMPT,
            "trend": TREND_REPORT_PROMPT,
        }
        system = prompt_map.get(report_type, REPORT_GENERATION_PROMPT)

        if topic:
            system += f"\n\n## 聚焦方向\n请重点关注与「{topic}」相关的文献。"

        # 构造论文输入
        paper_text = self._format_papers_for_llm(papers)

        client = AsyncOpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)

        try:
            response = await client.chat.completions.create(
                model=LLM_MODEL,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": paper_text},
                ],
                temperature=0.3,
                max_tokens=3000,
            )
            report = (response.choices[0].message.content or "").strip()
            if not report:
                return self._fallback_report(papers)

            logger.info(
                f"报告生成完成: {report_type}, "
                f"{len(papers)} 篇, {len(report)} 字"
            )
            return report

        except Exception as e:
            logger.error(f"报告生成 LLM 调用失败: {e}")
            return self._fallback_report(papers)

    def _format_papers_for_llm(self, papers: list[dict]) -> str:
        """将论文列表格式化为 LLM 输入文本"""
        lines = [f"共 {len(papers)} 篇文献:\n"]
        for i, p in enumerate(papers[:30], 1):  # 最多 30 篇
            title = p.get("title", "无标题")
            journal = p.get("journal", "")
            abstract = p.get("abstract", "")[:300]

            line = f"[{i}] 标题: {title}"
            if journal:
                line += f"\n    期刊: {journal}"
            if abstract:
                line += f"\n    摘要: {abstract}"
            lines.append(line)

        return "\n\n".join(lines)

    def _fallback_report(self, papers: list[dict]) -> str:
        """LLM 不可用时的降级报告"""
        lines = [
            f"## 文献批次摘要\n",
            f"共 {len(papers)} 篇文献\n",
        ]
        for i, p in enumerate(papers[:20], 1):
            title = p.get("title", "无标题")
            journal = p.get("journal", "")
            line = f"{i}. **{title}**"
            if journal:
                line += f" — {journal}"
            lines.append(line)

        if len(papers) > 20:
            lines.append(f"\n... 还有 {len(papers) - 20} 篇未列出")

        return "\n".join(lines)
