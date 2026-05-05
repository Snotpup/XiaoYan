"""
xiaoyan.core.settings_flow — 飞书交互式设置菜单
================================================
通过文本编号选择实现参数配置，用户无需接触配置文件。

流程: 用户发"设置" → 展示配置卡片(带编号) → 回复编号 → 展示选项 → 选择值 → 确认
"""

import logging
import time
from typing import Any

logger = logging.getLogger("xiaoyan.core.settings_flow")

# ── 设置项元数据 ──

SETTINGS_META = [
    {
        "key": "default_cron",
        "label": "爬取频率",
        "description": "新建检索式的默认定时频率",
        "options": [
            ("每 6 小时（推荐）", "0 */6 * * *"),
            ("每天早上 9 点", "0 9 * * *"),
            ("每 12 小时", "0 */12 * * *"),
            ("每天早晚各一次", "0 9,21 * * *"),
        ],
    },
    {
        "key": "cnki_max_pages",
        "label": "CNKI 最大页数",
        "description": "知网单次爬取的最大翻页数",
        "options": [
            ("3 页（推荐）", "3"),
            ("5 页", "5"),
            ("10 页", "10"),
        ],
    },
    {
        "key": "wos_max_records",
        "label": "WoS 最大条数",
        "description": "Web of Science 单次爬取的最大记录数",
        "options": [
            ("20 条（推荐）", "20"),
            ("50 条", "50"),
            ("100 条", "100"),
        ],
    },
    {
        "key": "daily_summary_time",
        "label": "每日摘要时间",
        "description": "每日爬取摘要的推送时间",
        "options": [
            ("每天 21:00（推荐）", "21:00"),
            ("每天 9:00", "9:00"),
            ("每天 8:00", "8:00"),
            ("每天 22:00", "22:00"),
        ],
    },
]

# 便捷索引
_SETTINGS_BY_KEY = {s["key"]: s for s in SETTINGS_META}

# cron 值 → 人类可读标签
_CRON_DISPLAY = {
    "0 */6 * * *": "每 6 小时",
    "0 9 * * *": "每天早上 9 点",
    "0 */12 * * *": "每 12 小时",
    "0 9,21 * * *": "每天早晚各一次",
}


def resolve_setting_display(key: str, value: str) -> str:
    """将设置值转为人类可读标签"""
    meta = _SETTINGS_BY_KEY.get(key)
    if meta:
        for label, opt_value in meta["options"]:
            if opt_value == value:
                return label
    if key == "default_cron":
        return _CRON_DISPLAY.get(value, value)
    if key == "cnki_max_pages":
        return f"{value} 页"
    if key == "wos_max_records":
        return f"{value} 条"
    if key == "daily_summary_time":
        return f"每天 {value}"
    return value


def get_meta_by_index(index: int) -> dict | None:
    """通过 1-based 编号获取设置项元数据"""
    if 1 <= index <= len(SETTINGS_META):
        return SETTINGS_META[index - 1]
    return None


def get_meta_by_key(key: str) -> dict | None:
    """通过 key 获取设置项元数据"""
    return _SETTINGS_BY_KEY.get(key)


def get_option_by_index(key: str, index: int) -> tuple[str, str] | None:
    """通过 1-based 编号获取某个设置项的可选值，返回 (label, value)"""
    meta = _SETTINGS_BY_KEY.get(key)
    if not meta:
        return None
    if 1 <= index <= len(meta["options"]):
        return meta["options"][index - 1]
    return None


# ── 对话状态管理 ──

TTL_SECONDS = 300  # 5 分钟

_CANCEL_KEYWORDS = {"取消", "退出", "cancel", "算了", "不改了", "返回"}


class SettingsFlow:
    """
    设置菜单的轻量对话状态管理。

    状态流转:
        None → {"step": "choose_setting"} → {"step": "choose_value", "key": "..."} → None（完成）
    """

    def __init__(self):
        # {chat_id: {"step": str, "key": str, "ts": float}}
        self._states: dict[str, dict[str, Any]] = {}

    def get_state(self, chat_id: str) -> dict | None:
        state = self._states.get(chat_id)
        if state and (time.time() - state["ts"] > TTL_SECONDS):
            del self._states[chat_id]
            logger.debug(f"设置流程超时清除: {chat_id}")
            return None
        return state

    def set_choose_setting(self, chat_id: str):
        """进入选择设置项的步骤"""
        self._states[chat_id] = {"step": "choose_setting", "ts": time.time()}

    def set_choose_value(self, chat_id: str, key: str):
        """进入选择值的步骤"""
        self._states[chat_id] = {"step": "choose_value", "key": key, "ts": time.time()}

    def clear(self, chat_id: str):
        self._states.pop(chat_id, None)

    def is_in_flow(self, chat_id: str) -> bool:
        return self.get_state(chat_id) is not None

    @staticmethod
    def is_cancel(text: str) -> bool:
        return text.strip().lower() in _CANCEL_KEYWORDS
