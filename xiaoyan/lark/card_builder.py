"""
xiaoyan.lark.card_builder — 飞书卡片消息模板
===============================================
构建结构化的飞书交互卡片 JSON。

用于:
  - 新文献推送 (标题 + 期刊 + LLM 评述)
  - 检索式列表展示
  - 操作确认卡片
"""

import json


def build_paper_card(
    title: str,
    journal: str,
    abstract: str,
    llm_comment: str,
    platform: str = "unknown",
    url: str = "",
) -> str:
    """
    构建单篇文献推送卡片。

    Args:
        title: 论文标题
        journal: 期刊名
        abstract: 摘要 (截断展示)
        llm_comment: LLM 一句话中文评述
        platform: 来源平台 (cnki/wos)
        url: 详情链接

    Returns:
        飞书交互卡片 JSON 字符串
    """
    platform_label = {"cnki": "知网", "wos": "Web of Science"}.get(platform, platform)
    abstract_preview = abstract[:200] + "..." if len(abstract) > 200 else abstract

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": f"📄 {title}"},
            "template": "blue",
        },
        "elements": [
            {
                "tag": "div",
                "fields": [
                    {"is_short": True, "text": {"tag": "lark_md", "content": f"**期刊**\n{journal}"}},
                    {"is_short": True, "text": {"tag": "lark_md", "content": f"**来源**\n{platform_label}"}},
                ],
            },
            {"tag": "hr"},
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**🤖 AI 评述**\n{llm_comment}"},
            },
            {"tag": "hr"},
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**摘要**\n{abstract_preview}"},
            },
        ],
    }

    # 添加"查看原文"按钮
    if url:
        card["elements"].append({
            "tag": "action",
            "actions": [
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "查看原文"},
                    "url": url,
                    "type": "primary",
                },
            ],
        })

    return json.dumps(card, ensure_ascii=False)


def build_papers_batch_card(
    papers: list[dict],
    query_desc: str = "",
    total_new: int = 0,
) -> str:
    """
    构建批量文献推送卡片 (汇总多篇)。

    Args:
        papers: [{"title": ..., "journal": ..., "llm_comment": ...}, ...]
        query_desc: 检索式描述
        total_new: 新增总数

    Returns:
        飞书交互卡片 JSON 字符串
    """
    header_text = f"📚 新增 {total_new} 篇文献"
    if query_desc:
        header_text += f" | {query_desc}"

    elements = []
    for i, paper in enumerate(papers[:10]):  # 最多展示 10 篇
        title = paper.get("title", "无标题")
        journal = paper.get("journal", "")
        comment = paper.get("llm_comment", "")

        content = f"**{i + 1}. {title}**"
        if journal:
            content += f"\n📖 {journal}"
        if comment:
            content += f"\n💡 {comment}"

        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": content},
        })
        if i < len(papers) - 1:
            elements.append({"tag": "hr"})

    if total_new > 10:
        elements.append({
            "tag": "note",
            "elements": [
                {"tag": "plain_text", "content": f"还有 {total_new - 10} 篇未展示"},
            ],
        })

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": header_text},
            "template": "green",
        },
        "elements": elements,
    }

    return json.dumps(card, ensure_ascii=False)


def build_query_list_card(queries: list[dict]) -> str:
    """
    构建检索式列表展示卡片。

    Args:
        queries: 从 db.list_search_queries() 获取的检索式列表

    Returns:
        飞书交互卡片 JSON 字符串
    """
    if not queries:
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": "📋 当前检索式列表"},
                "template": "indigo",
            },
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": "暂无任何检索式。\n发送\"添加检索\"来创建第一个！"}},
            ],
        }
        return json.dumps(card, ensure_ascii=False)

    elements = []
    for q in queries:
        platform_label = {"cnki": "知网", "wos": "WoS"}.get(q["platform"], q["platform"])
        status = "🟢 启用" if q.get("is_active", True) else "🔴 停用"

        content = f"**#{q['id']}** [{platform_label}] {status}"
        content += f"\n检索式: `{q['query'][:80]}`"
        if q.get("description"):
            content += f"\n描述: {q['description']}"
        content += f"\n定时: `{q.get('cron_expression', '0 */6 * * *')}`"

        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": content},
        })
        elements.append({"tag": "hr"})

    # 移除最后一个 hr
    if elements and elements[-1].get("tag") == "hr":
        elements.pop()

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": f"📋 检索式列表 ({len(queries)} 条)"},
            "template": "indigo",
        },
        "elements": elements,
    }

    return json.dumps(card, ensure_ascii=False)


def build_confirm_card(action: str, detail: str) -> str:
    """
    构建操作确认/结果卡片。

    Args:
        action: 操作描述 (如 "检索式已添加")
        detail: 详细信息

    Returns:
        飞书交互卡片 JSON 字符串
    """
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": f"✅ {action}"},
            "template": "green",
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": detail}},
        ],
    }

    return json.dumps(card, ensure_ascii=False)


def build_daily_summary_card(
    total_runs: int,
    success_runs: int,
    failed_runs: int,
    total_new_papers: int,
) -> str:
    """
    构建每日爬取摘要卡片 (P1-4)。

    Args:
        total_runs: 过去 24h 总执行次数
        success_runs: 成功次数
        failed_runs: 失败次数
        total_new_papers: 新增论文总数

    Returns:
        飞书交互卡片 JSON 字符串
    """
    from datetime import datetime
    date_str = datetime.now().strftime("%m月%d日")

    # 状态文本
    if failed_runs > 0:
        status_text = f"⚠️ 有 **{failed_runs}** 次失败"
    else:
        status_text = "✅ 全部成功"

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": f"📊 每日爬取摘要 — {date_str}"},
            "template": "purple",
        },
        "elements": [
            {
                "tag": "div",
                "fields": [
                    {
                        "is_short": True,
                        "text": {"tag": "lark_md", "content": f"**执行次数**\n{total_runs} 次"},
                    },
                    {
                        "is_short": True,
                        "text": {"tag": "lark_md", "content": f"**状态**\n{status_text}"},
                    },
                ],
            },
            {"tag": "hr"},
            {
                "tag": "div",
                "fields": [
                    {
                        "is_short": True,
                        "text": {"tag": "lark_md", "content": f"**成功**\n🟢 {success_runs} 次"},
                    },
                    {
                        "is_short": True,
                        "text": {"tag": "lark_md", "content": f"**新增论文**\n📄 {total_new_papers} 篇"},
                    },
                ],
            },
        ],
    }

    # 失败时追加提示
    if failed_runs > 0:
        card["elements"].append({"tag": "hr"})
        card["elements"].append({
            "tag": "note",
            "elements": [
                {
                    "tag": "plain_text",
                    "content": "💡 如果持续失败，请运行 `xiaoyan auth` 刷新浏览器认证",
                },
            ],
        })

    return json.dumps(card, ensure_ascii=False)


def build_settings_card(
    settings: dict,
    meta: list[dict],
    resolve_display,
) -> str:
    """
    构建系统设置总览卡片 (文本编号选项)。

    Args:
        settings: {"default_cron": "0 */6 * * *", ...}
        meta: SETTINGS_META 列表
        resolve_display: callable(key, value) -> str  将值转为可读标签

    Returns:
        飞书交互卡片 JSON 字符串
    """
    # 当前配置字段
    fields = []
    for item in meta:
        key = item["key"]
        raw_value = settings.get(key, "")
        display = resolve_display(key, raw_value)
        fields.append({
            "is_short": True,
            "text": {"tag": "lark_md", "content": f"**{item['label']}**\n{display}"},
        })

    # 编号选项文本
    options_lines = []
    for i, item in enumerate(meta, 1):
        options_lines.append(f"**{i}.** {item['label']}")
    options_text = "\n".join(options_lines)

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "⚙️ 系统设置"},
            "template": "blue",
        },
        "elements": [
            {"tag": "div", "fields": fields},
            {"tag": "hr"},
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**回复编号修改:**\n{options_text}"},
            },
            {
                "tag": "note",
                "elements": [
                    {"tag": "plain_text", "content": "回复「取消」退出设置"},
                ],
            },
        ],
    }

    return json.dumps(card, ensure_ascii=False)


def build_setting_options_card(
    key: str,
    label: str,
    current_value: str,
    options: list[tuple[str, str]],
    resolve_display,
) -> str:
    """
    构建设置选项卡片 (文本编号选项)。

    Args:
        key: 设置项 key
        label: 设置项中文标签
        current_value: 当前值
        options: [(显示标签, 存储值), ...]
        resolve_display: callable(key, value) -> str

    Returns:
        飞书交互卡片 JSON 字符串
    """
    current_display = resolve_display(key, current_value)

    # 编号选项文本
    options_lines = []
    for i, (opt_label, opt_value) in enumerate(options, 1):
        marker = " ✓" if opt_value == current_value else ""
        options_lines.append(f"**{i}.** {opt_label}{marker}")
    options_text = "\n".join(options_lines)

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": f"🔄 修改{label}"},
            "template": "indigo",
        },
        "elements": [
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"当前值: **{current_display}**"},
            },
            {"tag": "hr"},
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**回复编号选择:**\n{options_text}"},
            },
        ],
    }

    return json.dumps(card, ensure_ascii=False)


def build_setting_confirm_card(label: str, new_display: str) -> str:
    """
    构建设置修改确认卡片。

    Args:
        label: 设置项中文标签
        new_display: 新值的可读标签

    Returns:
        飞书交互卡片 JSON 字符串
    """
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "✅ 设置已更新"},
            "template": "green",
        },
        "elements": [
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**{label}** → {new_display}\n\n已生效，下次执行将使用新配置。"},
            },
        ],
    }

    return json.dumps(card, ensure_ascii=False)
