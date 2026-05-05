"""
xiaoyan.core.profile — 研究方向画像管理 (P1-2)
================================================
结构化管理用户的研究方向偏好，持久化到 YAML 文件。

职责:
  - 加载/保存画像 (~/.xiaoyan/research_profile.yaml)
  - 增量合并更新 (来自 chat 意图中的 profile_hint)
  - 格式化摘要注入意图识别 prompt
  - should_push() 硬规则过滤 (综述过滤等)

设计原则:
  - MVP 优先: 只保留高频使用的过滤维度
  - 宁可多推一篇不太相关的，也不要漏掉一篇重要的
  - 排除关键词等模糊匹配交给 LLM 语义判断，不在此做

参考: [P1-2-research-profile.md]
"""

import logging
from datetime import datetime
from pathlib import Path

import yaml

from xiaoyan.config import DATA_DIR

logger = logging.getLogger("xiaoyan.core.profile")

# 默认画像 (首次使用时创建)
_DEFAULT_PROFILE = {
    "updated_at": "",
    "domains": [],
    "keywords": {
        "preferred": [],
        "excluded": [],
    },
    "include_reviews": True,
    "platform_preference": "both",
}

# 综述检测关键词
_REVIEW_PATTERNS = [
    "a review", "a survey", "systematic review",
    "meta-analysis", "literature review",
    "综述", "研究进展", "研究综述", "文献综述",
]


class ResearchProfile:
    """
    研究方向画像管理器。

    用法:
        profile = ResearchProfile()
        data = profile.load()
        profile.update({"domains": ["绿色金融"]})
        if profile.should_push(paper):
            ...
    """

    def __init__(self):
        self._path: Path = DATA_DIR / "research_profile.yaml"

    def load(self) -> dict:
        """加载画像数据。文件不存在则返回默认画像。"""
        if self._path.exists():
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                # 补全缺失字段
                for k, v in _DEFAULT_PROFILE.items():
                    data.setdefault(k, v)
                return data
            except Exception as e:
                logger.error(f"画像文件加载失败，使用默认值: {e}")
                return dict(_DEFAULT_PROFILE)
        return dict(_DEFAULT_PROFILE)

    def update(self, changes: dict) -> dict:
        """
        增量合并更新画像。

        支持的 changes 字段:
          - domains: list[str]        → 覆盖替换
          - keywords_add: list[str]   → 追加到 preferred
          - keywords_exclude: list[str] → 追加到 excluded
          - include_reviews: bool     → 覆盖替换
          - platform_preference: str  → 覆盖替换

        Returns:
            更新后的完整画像
        """
        data = self.load()

        if "domains" in changes:
            new_domains = changes["domains"]
            if isinstance(new_domains, list):
                # 合并去重，保留已有的
                existing = set(data.get("domains", []))
                existing.update(new_domains)
                data["domains"] = list(existing)
            elif isinstance(new_domains, str):
                existing = set(data.get("domains", []))
                existing.add(new_domains)
                data["domains"] = list(existing)

        if "keywords_add" in changes:
            kw = data.setdefault("keywords", {"preferred": [], "excluded": []})
            existing = set(kw.get("preferred", []))
            added = changes["keywords_add"]
            if isinstance(added, list):
                existing.update(added)
            elif isinstance(added, str):
                existing.add(added)
            kw["preferred"] = list(existing)

        if "keywords_exclude" in changes:
            kw = data.setdefault("keywords", {"preferred": [], "excluded": []})
            existing = set(kw.get("excluded", []))
            excluded = changes["keywords_exclude"]
            if isinstance(excluded, list):
                existing.update(excluded)
            elif isinstance(excluded, str):
                existing.add(excluded)
            kw["excluded"] = list(existing)

        if "include_reviews" in changes:
            data["include_reviews"] = bool(changes["include_reviews"])

        if "platform_preference" in changes:
            val = changes["platform_preference"]
            if val in ("cnki", "wos", "both"):
                data["platform_preference"] = val

        data["updated_at"] = datetime.now().isoformat(timespec="seconds")
        self._save(data)

        logger.info(f"画像已更新: {changes}")
        return data

    def reset(self) -> dict:
        """重置为默认画像"""
        data = dict(_DEFAULT_PROFILE)
        data["updated_at"] = datetime.now().isoformat(timespec="seconds")
        self._save(data)
        logger.info("画像已重置为默认值")
        return data

    def format_summary(self, max_len: int = 400) -> str:
        """
        格式化画像摘要，用于注入意图识别 prompt。

        空画像返回空字符串 (不注入)。
        """
        data = self.load()
        lines = []

        domains = data.get("domains", [])
        if domains:
            lines.append(f"研究领域: {', '.join(domains)}")

        kw = data.get("keywords", {})
        preferred = kw.get("preferred", [])
        excluded = kw.get("excluded", [])
        if preferred:
            lines.append(f"偏好关键词: {', '.join(preferred)}")
        if excluded:
            lines.append(f"排除关键词: {', '.join(excluded)}")

        if not data.get("include_reviews", True):
            lines.append("不看综述类文献")

        pref = data.get("platform_preference", "both")
        if pref != "both":
            platform_label = {"cnki": "知网", "wos": "WoS"}.get(pref, pref)
            lines.append(f"平台偏好: {platform_label}")

        if not lines:
            return ""

        text = "\n".join(lines)
        return text[:max_len]

    def should_push(self, paper: dict) -> bool:
        """
        硬规则过滤 — 只处理高置信度的排除场景。

        原则: 宁可多推一篇不太相关的，也不要漏掉一篇重要的。
        更精细的相关性判断应交给 LLM (check_title_relevance)。
        """
        data = self.load()

        # 1. 综述过滤 (高置信度: 综述标题特征很明确)
        if not data.get("include_reviews", True):
            if _is_review(paper.get("title", "")):
                return False

        # 2. 排除关键词过滤 (子串匹配: 标题 + 摘要)
        excluded = data.get("keywords", {}).get("excluded", [])
        if excluded:
            text = (paper.get("title", "") + " " + paper.get("abstract", "")).lower()
            for kw in excluded:
                if kw.lower() in text:
                    return False

        return True

    def format_card_text(self) -> str:
        """格式化画像内容用于飞书消息展示"""
        data = self.load()
        lines = ["📋 **你的研究画像**\n"]

        domains = data.get("domains", [])
        if domains:
            lines.append(f"🔬 **研究领域**: {', '.join(domains)}")
        else:
            lines.append("🔬 **研究领域**: 未设置")

        kw = data.get("keywords", {})
        preferred = kw.get("preferred", [])
        excluded = kw.get("excluded", [])
        if preferred:
            lines.append(f"🏷 **偏好关键词**: {', '.join(preferred)}")
        if excluded:
            lines.append(f"🚫 **排除关键词**: {', '.join(excluded)}")

        review_label = "是" if data.get("include_reviews", True) else "否"
        lines.append(f"📖 **包含综述**: {review_label}")

        pref = data.get("platform_preference", "both")
        pref_label = {"cnki": "知网", "wos": "WoS", "both": "知网 + WoS"}.get(pref, pref)
        lines.append(f"🌐 **平台偏好**: {pref_label}")

        updated = data.get("updated_at", "")
        if updated:
            lines.append(f"\n⏰ 最后更新: {updated}")
        else:
            lines.append('\n💡 发送消息如"我研究绿色金融"、"我不看综述"来完善画像')

        return "\n".join(lines)

    def _save(self, data: dict):
        """持久化到 YAML 文件"""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as f:
            yaml.dump(
                data, f,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=False,
            )


def _is_review(title: str) -> bool:
    """检测标题是否为综述型文献"""
    title_lower = title.lower()
    return any(p in title_lower for p in _REVIEW_PATTERNS)
