"""
xiaoyan.core.intent — LLM 驱动的意图识别
============================================
接收用户自然语言消息，调用 LLM 判断意图类别。

支持的意图:
  - search: 临时检索 "帮我查一下 xxx"
  - add_query: 添加定时检索式
  - update_query: 修改检索式
  - delete_query: 删除检索式
  - list_queries: 查看所有检索式
  - chat: 闲聊、情绪表达或提供学术偏好（如“知道了”、“我不看综述”）
  - unknown: 完全无法识别

输出: 结构化 JSON {"intent": "...", "params": {...}}
"""

import json
import logging

from openai import AsyncOpenAI

from xiaoyan.config import LLM_API_KEY, LLM_BASE_URL, LLM_MODEL

logger = logging.getLogger("xiaoyan.core.intent")

# ============================================================
#  System Prompt
# ============================================================

INTENT_SYSTEM_PROMPT = """\
你是"小研"的意图识别模块。你的任务是分析用户的自然语言消息，判断其意图，并提取结构化参数。

## 意图列表

| 意图 | 触发条件 | 必需参数 |
|------|---------|---------|
| search | 用户想临时搜索文献，如"帮我查"、"找一下"、"搜索" | topic, platform(可选,默认wos) |
| deep_explore | 用户想做深度/全面的文献探索，如"深度探索"、"全面检索"、"帮我制定检索策略"、"系统梳理"、"文献图景" | topic, platform(可选,默认wos) |
| add_query | 用户想添加定时检索式，如"添加检索"、"新建"、"帮我追踪" | query(检索式), platform(可选), description(可选), cron(可选) |
| update_query | 用户想修改已有检索式，如"修改"、"更新"、"改一下" | query_id, 以及要更新的字段 |
| delete_query | 用户想删除检索式，如"删掉"、"移除"、"不要了" | query_id 或 description(用于模糊匹配) |
| list_queries | 用户想查看所有检索式，如"看看"、"列表"、"有哪些" | 无 |
| chat | 用户进行闲聊、确认回复("知道了")、或者补充自己的学术偏好("我不看综述"、"我研究绿色金融") | reply(你需要基于用户的语境或偏好，生成一句自然、贴心的回复，表明你已记下或理解), profile_hint(可选dict: 当用户表达了学术偏好时生成，字段包括 domains/keywords_add/keywords_exclude/include_reviews/platform_preference 中的变更项) |
| show_profile | 用户想查看自己的研究画像，如"看看我的画像"、"我的画像"、"我的研究方向" | 无 |
| generate_report | 用户想分析/总结/评述一批文献，如"总结最近的文献"、"分析今天的论文"、"做个文献综述" | source(可选: "recent"/"today"), report_type(可选: "summary"/"review"/"trend"), topic_filter(可选: 用户指定的主题) |
| settings | 用户想查看或修改系统设置，如"设置"、"配置"、"修改频率"、"调整参数"、"爬取设置" | action(可选: "view"/"modify"), setting_key(可选, 如"default_cron"/"cnki_max_pages"/"wos_max_records") |
| unknown | 完全无法归类且毫无意义的乱码输入 | 无 |

## search vs deep_explore 区分规则
- **search** 是默认选择，适用于绝大多数文献检索场景（"帮我查"、"找一下"）
- **deep_explore** 仅在用户明确表达需要**系统性、多轮、全面**的检索策略时使用
- 触发 deep_explore 的关键词: "深度探索"、"全面检索"、"制定检索策略"、"系统梳理"、"文献图景"、"帮我规划检索"
- 不确定时，**默认使用 search**

## 平台映射
- 知网/CNKI/中文文献 → platform: "cnki"
- WoS/Web of Science/英文文献/SCI → platform: "wos"
- **当用户的主题描述为中文时（如"银行信贷""人工智能"），默认选 platform: "cnki"**
- **只有当主题明确是英文或用户指定 WoS/SCI 时，才选 platform: "wos"**
- 如果用户同时需要中英文，返回 platform: "cnki"（中文优先）

## 检索式生成规则
- 当用户给出自然语言主题时 (如"AI 医疗")，你需要帮他生成对应平台的检索式
- 如果用户直接给出了检索式格式 (如 "TS=(AI AND drug)")，原样保留

### CNKI 检索式格式 (严格遵守)
- 字段代码: SU=主题, TI=篇名, KY=关键词, AB=摘要, AU=作者, AF=机构
- 逻辑运算符: AND, OR, NOT — **前后必须有空格**
- 匹配运算符: = 精确匹配, %= 模糊匹配
- 组合多个主题词: SU=('关键词1' AND '关键词2')
- 同义词扩展: (SU='AI' OR SU='人工智能') AND (SU='医疗' OR SU='医疗卫生')
- 示例:
  - 简单主题: SU=('人工智能' AND '医疗')
  - 带同义词: (SU='AI' OR SU='人工智能') AND SU='医疗'
  - 排除: SU='大数据' NOT TI='大数据集'
- **禁止**: 运算符前后不加空格 (如 AND/OR/NOT 紧邻引号)

### WoS 检索式格式
- TS=(keyword1 AND keyword2) — 主题检索
- 示例: TS=(artificial intelligence AND healthcare)

## 时间范围约束（重要）
- **除非用户明确指定了时间范围**，生成的检索式应默认限定为**最近文献**
- 对于 search 意图（临时检索），默认限定最近 30 天
- 对于 add_query 意图（定时追踪），可以不限时间（定时任务自身会增量更新）
- 在 params 中添加 `"date_range"` 字段:
  - "recent_30d" (默认，最近 30 天)
  - "recent_7d" (一周内)
  - "recent_year" (近一年)
  - "all" (用户明确要求全量时)
  - 或用户指定的具体范围如 "2024-2025"

## 输出格式 (严格 JSON)

```json
{
  "intent": "search",
  "params": {
    "topic": "用户原始描述",
    "query": "生成的检索式",
    "platform": "wos"
  },
  "confidence": 0.95
}
```

只输出 JSON，不要有其他文字。
"""


# ============================================================
#  意图识别
# ============================================================

async def recognize_intent(
    user_message: str,
    *,
    recent_context: str = "",
    memory_summary: str = "",
) -> dict:
    """
    调用 LLM 识别用户意图。

    Args:
        user_message: 用户原始消息文本
        recent_context: 最近几轮对话摘要 (滑动窗口)
        memory_summary: 会话记忆摘要 (持久化笔记)

    Returns:
        {
            "intent": "search" | "add_query" | "update_query" |
                      "delete_query" | "list_queries" | "chat" | "unknown",
            "params": {...},
            "confidence": 0.0-1.0
        }
    """
    if not LLM_API_KEY:
        logger.warning("LLM API Key 未配置，意图识别降级为 unknown")
        return {"intent": "unknown", "params": {}, "confidence": 0.0}

    client = AsyncOpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)

    # 构建带上下文的 system prompt
    system = INTENT_SYSTEM_PROMPT
    if recent_context:
        system += f"\n\n## 最近对话上下文\n{recent_context}"
    if memory_summary:
        system += f"\n\n## 会话记忆\n{memory_summary}"

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user_message},
    ]

    try:
        # 优先尝试 json_object 模式 (OpenAI/DeepSeek 支持)
        try:
            response = await client.chat.completions.create(
                model=LLM_MODEL,
                messages=messages,
                temperature=0.1,
                max_tokens=500,
                response_format={"type": "json_object"},
            )
            raw = (response.choices[0].message.content or "").strip()
            if raw:
                result = json.loads(raw)
            else:
                raise ValueError("json_object 模式返回空内容")
        except (ValueError, json.JSONDecodeError):
            # Fallback: 不使用 json_object 模式，手动提取 JSON
            response = await client.chat.completions.create(
                model=LLM_MODEL,
                messages=messages,
                temperature=0.1,
                max_tokens=500,
            )
            raw = (response.choices[0].message.content or "").strip()
            result = _extract_json(raw)

        # 确保必需字段存在
        result.setdefault("intent", "unknown")
        result.setdefault("params", {})
        result.setdefault("confidence", 0.5)

        logger.info(
            f"意图识别: [{result['intent']}] "
            f"(置信度 {result['confidence']:.0%}) "
            f"| 用户消息: {user_message[:50]}..."
        )

        return result

    except json.JSONDecodeError as e:
        logger.error(f"LLM 返回非 JSON: {e}")
        return {"intent": "unknown", "params": {}, "confidence": 0.0}
    except Exception as e:
        logger.error(f"意图识别调用失败: {e}")
        return {"intent": "unknown", "params": {}, "confidence": 0.0}


def _extract_json(text: str) -> dict:
    """从 LLM 返回文本中提取 JSON 对象（处理 markdown 代码块包裹等情况）"""
    import re
    # 尝试直接解析
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # 尝试从 ```json ... ``` 代码块中提取
    match = re.search(r'```(?:json)?\s*\n?(.*?)\n?\s*```', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1).strip())
        except json.JSONDecodeError:
            pass

    # 尝试找到第一个 { ... } 块
    match = re.search(r'\{[^{}]*\}', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    raise json.JSONDecodeError("无法从 LLM 输出中提取 JSON", text, 0)


# ============================================================
#  LLM 文献评述
# ============================================================

COMMENT_SYSTEM_PROMPT = """\
你是一位学术助理。请用一句简洁的中文评述这篇论文的核心贡献和研究价值。
要求：
- 用中文回答
- 一句话，15-50 字
- 突出方法创新或应用价值
- 不要写"本文"开头，直接陈述
{profile_context}"""


async def generate_paper_comment(
    title: str, abstract: str, profile_summary: str = ""
) -> str:
    """
    对单篇论文生成 LLM 一句话中文评述。

    Args:
        title: 论文标题
        abstract: 论文摘要
        profile_summary: 用户研究画像摘要，用于生成个性化评述

    Returns:
        一句话中文评述
    """
    if not LLM_API_KEY:
        return "（LLM 未配置，暂无评述）"

    client = AsyncOpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)

    profile_context = ""
    if profile_summary:
        profile_context = (
            f"\n- 结合以下用户研究方向进行关联性点评:\n"
            f"{profile_summary}"
        )

    system_prompt = COMMENT_SYSTEM_PROMPT.format(profile_context=profile_context)

    try:
        response = await client.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"标题: {title}\n摘要: {abstract[:500]}"},
            ],
            temperature=0.3,
            max_tokens=100,
        )
        comment = response.choices[0].message.content.strip()
        comment = comment.strip('"\'')
        return comment

    except Exception as e:
        logger.error(f"文献评述生成失败: {e}")
        return "（评述生成失败）"


# ============================================================
#  LLM 检索式优化 (Adaptive Search 专用)
# ============================================================

REFINE_QUERY_SYSTEM_PROMPT = """\
你是学术文献检索专家。用户给你上一轮检索的反馈，请你优化检索式。

## 平台语法
- **WoS**: TS=(keyword1 AND keyword2), 使用通配符如 firm*, compan*
  - 必须使用**国际学术标准英文术语**，不要直译中文
  - 例: "僵尸企业" → zombie firm*, NOT zombie enterprise
- **CNKI (知网专业检索)**:
  - 字段代码: SU=主题, TI=篇名, KY=关键词, AB=摘要
  - 逻辑运算符: AND, OR, NOT — **前后必须有空格**
  - 简单主题: SU=('人工智能' AND '医疗')
  - 带同义词: (SU='AI' OR SU='人工智能') AND SU='医疗'
  - **禁止**: 运算符前后不加空格 (如 AND/OR/NOT 紧邻引号)
  - **禁止**: 使用 + 作为逻辑运算符

## 优化策略
- 零结果 → 放宽: 减少限定词、使用通配符、尝试同义词
- 结果过多 (>5000) → 加限定: 增加 AND 条件、加时间范围
- 标题偏题 → 换词: 调整核心关键词组合
- 结果偏少但相关 → 适度放宽: 增加 OR 同义词
- **CNKI 零结果时**: 优先检查语法是否正确 (AND/OR 前后有空格)，再尝试简化检索式

## 输出格式 (严格 JSON)
```json
{
  "query": "优化后的检索式",
  "adjustment_reason": "本次调整的原因",
  "expected_improvement": "预期改善效果"
}
```
只输出 JSON，不要有其他文字。
"""


async def refine_search_query(
    original_query: str,
    platform: str,
    round_result: dict,
    topic: str,
) -> dict:
    """
    让 LLM 根据上一轮检索反馈优化检索式。

    Args:
        original_query: 上一轮使用的检索式
        platform: "wos" 或 "cnki"
        round_result: {
            "total_found": int,
            "papers_count": int,
            "sample_titles": list[str],
            "failure_reason": str,
        }
        topic: 用户原始主题描述

    Returns:
        {"query": str, "adjustment_reason": str, "expected_improvement": str}
    """
    if not LLM_API_KEY:
        return {"query": original_query, "adjustment_reason": "LLM 未配置", "expected_improvement": ""}

    client = AsyncOpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)

    user_msg = (
        f"用户研究主题: {topic}\n"
        f"平台: {platform.upper()}\n"
        f"上一轮检索式: {original_query}\n"
        f"结果总数: {round_result.get('total_found', 0)}\n"
        f"实际提取数: {round_result.get('papers_count', 0)}\n"
        f"问题: {round_result.get('failure_reason', '未知')}\n"
    )
    titles = round_result.get("sample_titles", [])
    if titles:
        user_msg += f"样本标题:\n" + "\n".join(f"  - {t}" for t in titles[:10])

    try:
        messages = [
            {"role": "system", "content": REFINE_QUERY_SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ]

        # 优先尝试 json_object 模式
        try:
            response = await client.chat.completions.create(
                model=LLM_MODEL,
                messages=messages,
                temperature=0.2,
                max_tokens=500,
                response_format={"type": "json_object"},
            )
            raw = (response.choices[0].message.content or "").strip()
            if not raw:
                raise ValueError("json_object 模式返回空内容 (可能被 thinking 消耗)")
            result = json.loads(raw)
        except (ValueError, json.JSONDecodeError) as fallback_err:
            logger.warning(f"json_object 模式失败 ({fallback_err})，降级为普通模式")
            response = await client.chat.completions.create(
                model=LLM_MODEL,
                messages=messages,
                temperature=0.2,
                max_tokens=500,
            )
            raw = (response.choices[0].message.content or "").strip()
            result = _extract_json(raw) if raw else {}
        result.setdefault("query", original_query)
        result.setdefault("adjustment_reason", "")
        result.setdefault("expected_improvement", "")
        logger.info(
            f"检索式优化: {original_query[:30]}... → {result['query'][:30]}... "
            f"| 原因: {result['adjustment_reason'][:50]}"
        )
        return result
    except Exception as e:
        logger.error(f"检索式优化 LLM 调用失败: {e}")
        return {"query": original_query, "adjustment_reason": f"LLM 调用失败: {e}", "expected_improvement": ""}


# ============================================================
#  LLM 标题相关性抽检 (Adaptive Search 专用)
# ============================================================

RELEVANCE_CHECK_SYSTEM_PROMPT = """\
你是学术文献相关性判定专家。给定一个研究主题和一组论文标题，判断每个标题是否与该主题相关。

"相关" 的标准:
- 直接研究该主题
- 研究该主题的子领域
- 该主题是论文的核心变量或研究对象
- 方法论上与该主题高度相关

"不相关" 的标准:
- 仅在标题中提及但非核心
- 完全不同的研究领域
- 虽然包含相同关键词但含义不同

输出格式 (严格 JSON):
```json
{
  "results": [true, false, true, ...]
}
```
结果数组长度必须与输入标题数量一致。只输出 JSON。
"""


async def check_title_relevance(
    titles: list[str],
    topic: str,
) -> list[bool]:
    """
    用 LLM 判断每个标题是否与主题相关。

    Args:
        titles: 论文标题列表 (最多 10 个)
        topic: 用户研究主题

    Returns:
        布尔列表，True = 相关
    """
    if not titles:
        return []

    if not LLM_API_KEY:
        logger.warning("LLM 未配置，相关性检查默认全部为 True")
        return [True] * len(titles)

    client = AsyncOpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)

    # 最多送 10 个标题
    check_titles = titles[:10]
    user_msg = (
        f"研究主题: {topic}\n\n"
        f"论文标题 ({len(check_titles)} 篇):\n"
        + "\n".join(f"{i+1}. {t}" for i, t in enumerate(check_titles))
    )

    try:
        messages = [
            {"role": "system", "content": RELEVANCE_CHECK_SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ]

        # 优先尝试 json_object 模式
        try:
            response = await client.chat.completions.create(
                model=LLM_MODEL,
                messages=messages,
                temperature=0.1,
                max_tokens=500,
                response_format={"type": "json_object"},
            )
            raw = (response.choices[0].message.content or "").strip()
            if not raw:
                raise ValueError("json_object 模式返回空内容 (可能被 thinking 消耗)")
            result = json.loads(raw)
        except (ValueError, json.JSONDecodeError) as fallback_err:
            logger.warning(f"json_object 模式失败 ({fallback_err})，降级为普通模式")
            # 降级: 不使用 response_format，手动提取 JSON
            response = await client.chat.completions.create(
                model=LLM_MODEL,
                messages=messages,
                temperature=0.1,
                max_tokens=500,
            )
            raw = (response.choices[0].message.content or "").strip()
            result = _extract_json(raw) if raw else {}
        relevance = result.get("results", [])

        # 安全校验: 长度必须匹配
        if len(relevance) != len(check_titles):
            logger.warning(
                f"相关性结果长度不匹配: 期望 {len(check_titles)}, "
                f"得到 {len(relevance)}, 默认全部为 True"
            )
            return [True] * len(check_titles)

        relevant_count = sum(1 for r in relevance if r)
        logger.info(
            f"标题相关性: {relevant_count}/{len(check_titles)} "
            f"({relevant_count/len(check_titles):.0%}) | 主题: {topic[:30]}"
        )
        return relevance

    except Exception as e:
        logger.error(f"标题相关性 LLM 调用失败: {e}")
        return [True] * len(check_titles)


# ============================================================
#  LLM 课题分解 (深度探索模式专用)
#  迁移自智研TRACK 知网文献检索.py 的概念分解阶段
#  增量改进: 同时生成 CNKI + WoS 双平台探测检索式
# ============================================================

DECOMPOSE_TOPIC_PROMPT = """\
你是学术文献检索策略专家。请分析用户的研究课题，拆解为核心概念，并生成初始探测检索式。

## 要求
1. 识别课题中的核心概念/变量（**至少 2 个，通常 2-4 个**）
2. 为**每个概念独立**生成探测检索式（同时提供 CNKI 和 WoS 两个平台版本）
3. 再生成 1-2 个**概念组合**的探测检索式（测试概念交叉的文献存量）
4. initial_probes **至少 3 个**

## 平台检索语法（严格遵守）

### CNKI 专业检索语法
- **`*` 是 AND**（同时包含）: SU=('绿色金融' * '僵尸企业')
- **`+` 是 OR**（任一包含）: SU=('绿色金融' + '绿色信贷')
-  字段: SU=主题, TI=标题, KY=关键词
- ⚠ 绝对不要在 cnki_query 中使用 TS=、AND、OR 等 WoS 语法

### WoS 高级检索语法
- 格式: TS=(keyword1 AND keyword2)
- **必须使用国际学术标准英文术语**
- ⚠ 绝对不要在 wos_query 中使用 SU=、*、+ 等 CNKI 语法

## 输出格式 (严格 JSON)
```json
{
  "core_concepts": [
    {"concept": "概念名称", "role": "在课题中的角色（如自变量/因变量/调节变量/研究对象）"}
  ],
  "initial_probes": [
    {
      "probe_id": "P_001",
      "description": "探测目的简述",
      "cnki_query": "SU=('中文关键词')",
      "wos_query": "TS=(English keywords)",
      "rationale": "选择此检索词的理由"
    }
  ]
}
```
只输出 JSON，不要有其他文字。
"""


async def decompose_topic(topic: str) -> dict:
    """
    用 LLM 将研究课题分解为核心概念 + 初始探测检索式。

    迁移自智研TRACK 知网文献检索.py 的 identify_core_concepts 阶段,
    增量改进: 同时生成 CNKI + WoS 双平台检索式。

    Args:
        topic: 用户研究课题描述

    Returns:
        {
            "core_concepts": [{"concept": str, "role": str}, ...],
            "initial_probes": [{"probe_id": str, "cnki_query": str, "wos_query": str, ...}, ...]
        }
    """
    if not LLM_API_KEY:
        return {"core_concepts": [], "initial_probes": []}

    client = AsyncOpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)

    try:
        try:
            response = await client.chat.completions.create(
                model=LLM_MODEL,
                messages=[
                    {"role": "system", "content": DECOMPOSE_TOPIC_PROMPT},
                    {"role": "user", "content": f"研究课题: {topic}"},
                ],
                temperature=0.2,
                max_tokens=1500,
                response_format={"type": "json_object"},
            )
            raw = (response.choices[0].message.content or "").strip()
            if not raw:
                raise ValueError("json_object 模式返回空内容")
            result = json.loads(raw)
        except (ValueError, json.JSONDecodeError):
            response = await client.chat.completions.create(
                model=LLM_MODEL,
                messages=[
                    {"role": "system", "content": DECOMPOSE_TOPIC_PROMPT},
                    {"role": "user", "content": f"研究课题: {topic}"},
                ],
                temperature=0.2,
                max_tokens=1500,
            )
            raw = (response.choices[0].message.content or "").strip()
            result = _extract_json(raw) if raw else {}

        result.setdefault("core_concepts", [])
        result.setdefault("initial_probes", [])

        logger.info(
            f"课题分解: {len(result['core_concepts'])} 个概念, "
            f"{len(result['initial_probes'])} 个探测检索式"
        )
        return result

    except Exception as e:
        logger.error(f"课题分解 LLM 调用失败: {e}")
        return {"core_concepts": [], "initial_probes": []}


# ============================================================
#  LLM 探测决策 (深度探索模式专用)
#  迁移自智研TRACK 知网文献检索.py 的迭代决策逻辑
# ============================================================

EXPLORE_DECISION_PROMPT_TEMPLATE = """\
你是学术文献检索策略专家。基于目前的探测结果，决定下一步策略。

## 当前操作平台: {platform_upper}

## 平台检索语法（严格遵守，不可混用）

### CNKI 专业检索
- **`*` 是 AND**: SU=('词1' * '词2')  ← 同时包含
- **`+` 是 OR**: SU=('词1' + '词2')  ← 任一包含
- 字段: SU=主题, TI=标题, KY=关键词
- ⚠ CNKI 绝对不能使用 TS=、AND、OR、英文检索式

### WoS 高级检索
- 格式: TS=(keyword1 AND keyword2)
- 必须使用国际学术标准英文术语
- ⚠ WoS 绝对不能使用 SU=、*、+ 等 CNKI 语法

## 决策规则
你需要分析当前探测的结果，决定:
- **continue**: 需要进一步探测（换维度/换关键词/组合概念）
- **finalize**: 探测已充分，可以生成最终检索策略报告

⚠ 重要:
- 如果连续多轮结果数量相近且没有发现新维度，应果断 finalize
- **只输出当前平台 ({platform_upper}) 对应格式的检索式**
- 不要重复已经探测过的检索式

## 输出格式 (严格 JSON)

如果需要继续探测:
```json
{{
  "decision": "continue",
  "analysis": "对当前探测结果的分析",
  "next_probe": {{
    "probe_id": "P_XXX",
    "description": "下一步探测目的",
    "{query_field}": "当前平台格式的检索式",
    "rationale": "为什么需要这个探测"
  }}
}}
```

如果探测充分，需要总结:
```json
{{
  "decision": "finalize",
  "analysis": "为什么判断探测已充分",
  "final_strategy": {{
    "summary": "整体检索策略概述",
    "recommended_queries": [
      {{
        "category": "分类名称",
        "cnki_query": "SU=('推荐检索式')",
        "wos_query": "TS=(recommended query)",
        "expected_papers": "预期文献范围"
      }}
    ],
    "advice": ["对研究者的建议1", "建议2"]
  }}
}}
```
只输出 JSON，不要有其他文字。
"""


async def decide_next_explore_step(
    topic: str,
    probe_history: list[dict],
    platform: str = "wos",
) -> dict:
    """
    基于已有探测历史，LLM 决定下一步: 继续探测 or 生成最终策略。

    Args:
        topic: 研究课题
        probe_history: 探测历史列表, 每项:
            {"probe_id": str, "query": str, "platform": str,
             "total_found": int, "sample_titles": list[str]}
        platform: 当前操作平台 ("wos" 或 "cnki")

    Returns:
        {"decision": "continue"|"finalize", ...}
    """
    if not LLM_API_KEY:
        return {"decision": "finalize", "analysis": "LLM 未配置", "final_strategy": {}}

    client = AsyncOpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)

    # 动态填充平台信息到 prompt
    query_field = "cnki_query" if platform == "cnki" else "wos_query"
    system_prompt = EXPLORE_DECISION_PROMPT_TEMPLATE.format(
        platform_upper=platform.upper(),
        query_field=query_field,
    )

    # 构建探测历史摘要
    history_text = f"研究课题: {topic}\n当前平台: {platform.upper()}\n\n已完成的探测:\n"
    for h in probe_history:
        history_text += (
            f"\n--- {h['probe_id']} ({h['platform'].upper()}) ---\n"
            f"检索式: {h['query']}\n"
            f"结果总数: {h['total_found']}\n"
        )
        if h.get("error"):
            history_text += f"错误: {h['error']}\n"
        titles = h.get("sample_titles", [])
        if titles:
            history_text += "样本标题:\n"
            history_text += "\n".join(f"  [{i+1}] {t}" for i, t in enumerate(titles[:8]))
            history_text += "\n"

    try:
        try:
            response = await client.chat.completions.create(
                model=LLM_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": history_text},
                ],
                temperature=0.2,
                max_tokens=2000,
                response_format={"type": "json_object"},
            )
            raw = (response.choices[0].message.content or "").strip()
            if not raw:
                raise ValueError("json_object 模式返回空内容")
            result = json.loads(raw)
        except (ValueError, json.JSONDecodeError):
            response = await client.chat.completions.create(
                model=LLM_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": history_text},
                ],
                temperature=0.2,
                max_tokens=2000,
            )
            raw = (response.choices[0].message.content or "").strip()
            result = _extract_json(raw) if raw else {}

        result.setdefault("decision", "finalize")
        result.setdefault("analysis", "")
        logger.info(f"探测决策: {result['decision']} | {result['analysis'][:60]}")
        return result

    except Exception as e:
        logger.error(f"探测决策 LLM 调用失败: {e}")
        return {"decision": "finalize", "analysis": f"LLM 调用失败: {e}", "final_strategy": {}}
