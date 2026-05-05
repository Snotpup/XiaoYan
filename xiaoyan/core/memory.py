"""
xiaoyan.core.memory — 轻量对话记忆管理
==========================================
借鉴 Claude Code 的 Session Memory 设计，提供两层记忆:

  Layer 1: 滑动窗口 (ConversationContext)
    - 保留最近 N 轮原始消息
    - 每次意图识别时注入最近几轮作为上下文
    - 零 LLM 成本，纯内存操作

  Layer 2: 会话记忆文件 (SessionMemory)
    - 结构化 Markdown 文件 (~/.xiaoyan/session_memory.md)
    - 由 LLM 定期自动提取对话要点并增量更新
    - 跨重启可恢复，4 个固定 section，上限 2K tokens

设计原则:
  - 轻量化: 不 fork 子进程，直接异步 LLM 调用
  - 双阈值触发: 消息数 + 时间间隔，避免过度调用
  - 结构化模板: Session Memory 有固定 section，LLM 只更新内容不改结构

参考: Claude Code SessionMemory/sessionMemory.ts
"""

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

from openai import AsyncOpenAI

from xiaoyan.config import DATA_DIR, LLM_API_KEY, LLM_BASE_URL, LLM_MODEL

logger = logging.getLogger("xiaoyan.core.memory")


# ============================================================
#  Layer 1: 滑动窗口
# ============================================================

@dataclass
class Turn:
    """一轮对话"""
    role: str          # "user" | "assistant"
    content: str       # 消息原文
    timestamp: float   # time.time()
    intent: str = ""   # 识别到的意图 (仅 user 消息)

    def to_line(self, max_len: int = 200) -> str:
        label = "用户" if self.role == "user" else "小研"
        text = self.content[:max_len]
        suffix = f" [{self.intent}]" if self.intent else ""
        return f"{label}{suffix}: {text}"


class ConversationContext:
    """
    滑动窗口式对话上下文。

    保留最近 MAX_TURNS 轮原始消息，为意图识别提供
    短期记忆，解决"中文的也需要"等上下文依赖问题。
    """

    MAX_TURNS = 10  # 保留轮数 (user+assistant 各算 1 轮)

    def __init__(self):
        self._turns: list[Turn] = []

    def add_user(self, content: str, intent: str = ""):
        """记录用户消息"""
        self._turns.append(Turn(
            role="user",
            content=content,
            timestamp=time.time(),
            intent=intent,
        ))
        self._trim()

    def add_assistant(self, content: str):
        """记录bot回复摘要"""
        self._turns.append(Turn(
            role="assistant",
            content=content,
            timestamp=time.time(),
        ))
        self._trim()

    def _trim(self):
        """保持窗口大小"""
        if len(self._turns) > self.MAX_TURNS * 2:
            self._turns = self._turns[-(self.MAX_TURNS * 2):]

    def format_recent(self, n: int = 6) -> str:
        """
        格式化最近 n 条消息，用于注入意图识别 prompt。

        返回空字符串表示没有历史上下文 (第一条消息)。
        """
        recent = self._turns[-n:]
        if len(recent) <= 1:
            return ""
        # 不包含最后一条 (那是当前正在分析的消息)
        history = recent[:-1] if recent[-1].role == "user" else recent
        if not history:
            return ""
        return "\n".join(t.to_line() for t in history)

    @property
    def turn_count(self) -> int:
        return len(self._turns)

    @property
    def last_user_turn(self) -> Turn | None:
        for t in reversed(self._turns):
            if t.role == "user":
                return t
        return None


# ============================================================
#  Layer 2: 会话记忆文件
# ============================================================

SESSION_MEMORY_TEMPLATE = """\
# 会话记忆

## 当前状态
_用户当前正在做什么？最近的请求和进展。_

## 研究主题
_用户关注的研究领域、关键词和检索偏好。_

## 检索历史
_执行过的检索及简要结果。_

## 用户偏好
_用户的平台偏好、语言偏好、反馈记录。_
"""

SESSION_MEMORY_UPDATE_PROMPT = """\
你是一个笔记助手。根据下面的对话记录，更新会话记忆文件。

## 规则
1. 保留所有 section 标题（# 开头的行）和斜体描述行（_..._ 格式）不变
2. 只更新每个 section 的实际内容
3. 每个 section 的内容不超过 300 字
4. 如果某个 section 没有新信息，保持原样不改
5. "当前状态" section 必须反映最新进展
6. 不要添加新的 section
7. 输出完整的更新后文件内容（从 "# 会话记忆" 开始）

## 记忆职责区分 (重要)
- **ResearchProfile** (research_profile.yaml): 长期稳定的用户画像
  - 研究领域、偏好关键词、排除关键词、综述偏好、平台偏好
  - 由用户明确表达偏好时自动更新 ("我不看综述"、"我研究绿色金融")
- **SessionMemory** (本文件): 当前会话的动态上下文
  - 当前检索状态、检索历史、用户临时反馈、会话进展
  - 不要记录 ResearchProfile 中已有的信息（研究方向、排除关键词等）

## 当前记忆文件
{current_memory}

## 最近对话记录
{conversation}
"""


class SessionMemory:
    """
    会话记忆文件管理器。

    在 ~/.xiaoyan/session_memory.md 维护结构化笔记，
    由 LLM 定期提取对话要点并增量更新。

    触发策略 (借鉴 Claude Code 双阈值):
      - 至少处理 MESSAGES_THRESHOLD 条消息 AND
      - 距上次更新至少 MIN_INTERVAL_SECONDS 秒
    """

    MESSAGES_THRESHOLD = 5
    MIN_INTERVAL_SECONDS = 180  # 3 分钟

    def __init__(self):
        self._path = DATA_DIR / "session_memory.md"
        self._messages_since_update = 0
        self._last_update_time = 0.0
        self._updating = False

    def load(self) -> str:
        """加载当前记忆文件内容"""
        if self._path.exists():
            return self._path.read_text(encoding="utf-8")
        return SESSION_MEMORY_TEMPLATE

    def load_summary(self, max_len: int = 800) -> str:
        """
        加载记忆摘要，用于注入意图识别 prompt。

        只返回有实际内容的 section，跳过空 section。
        """
        content = self.load()
        # 过滤掉只有模板描述没有实际内容的 section
        lines = content.split("\n")
        result = []
        in_empty_section = False

        for i, line in enumerate(lines):
            if line.startswith("## "):
                # 检查这个 section 后面是否有实际内容
                section_content = []
                for j in range(i + 1, len(lines)):
                    if lines[j].startswith("## ") or lines[j].startswith("# "):
                        break
                    stripped = lines[j].strip()
                    # 跳过斜体描述行和空行
                    if stripped and not (stripped.startswith("_") and stripped.endswith("_")):
                        section_content.append(stripped)

                in_empty_section = len(section_content) == 0
                if not in_empty_section:
                    result.append(line)
            elif line.startswith("# "):
                in_empty_section = False
                result.append(line)
            elif not in_empty_section:
                stripped = line.strip()
                # 保留非斜体描述的非空行
                if stripped and not (stripped.startswith("_") and stripped.endswith("_")):
                    result.append(line)

        text = "\n".join(result).strip()
        return text[:max_len] if text else ""

    def tick(self):
        """每处理一条用户消息时调用，累加计数器"""
        self._messages_since_update += 1

    def should_update(self) -> bool:
        """是否应该触发 LLM 更新"""
        if self._updating:
            return False
        if self._messages_since_update < self.MESSAGES_THRESHOLD:
            return False
        elapsed = time.time() - self._last_update_time
        if elapsed < self.MIN_INTERVAL_SECONDS:
            return False
        return True

    async def update(self, context: ConversationContext):
        """
        使用 LLM 更新会话记忆文件。

        在后台异步执行，不阻塞主消息处理流程。
        """
        if not LLM_API_KEY or self._updating:
            return

        self._updating = True
        try:
            current = self.load()
            conversation = "\n".join(
                t.to_line(max_len=300) for t in context._turns[-20:]
            )

            if not conversation.strip():
                return

            client = AsyncOpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)

            response = await client.chat.completions.create(
                model=LLM_MODEL,
                messages=[
                    {"role": "user", "content": SESSION_MEMORY_UPDATE_PROMPT.format(
                        current_memory=current,
                        conversation=conversation,
                    )},
                ],
                temperature=0.2,
                max_tokens=1500,
            )

            updated = (response.choices[0].message.content or "").strip()

            # 基本校验: 确保 LLM 输出仍然有正确的结构
            if "# 会话记忆" in updated and "## 当前状态" in updated:
                self._path.write_text(updated, encoding="utf-8")
                logger.info(f"会话记忆已更新 ({len(updated)} 字)")
            else:
                logger.warning("LLM 输出格式不符，跳过更新")

            self._messages_since_update = 0
            self._last_update_time = time.time()

        except Exception as e:
            logger.error(f"会话记忆更新失败: {e}")
        finally:
            self._updating = False
