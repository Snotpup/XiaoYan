"""
xiaoyan.core.utils — 通用工具函数
"""


def looks_chinese(text: str) -> bool:
    """检测文本是否包含实质性中文内容 (>30% 为中文字符)"""
    if not text:
        return False
    cjk = sum(1 for c in text if '一' <= c <= '鿿')
    alpha = sum(1 for c in text if c.isalpha())
    return alpha > 0 and cjk / alpha > 0.3


def apply_date_range(query: str, platform: str, date_range: str) -> str:
    """
    根据 date_range 参数为检索式追加时间限定。

    CNKI: 不在检索式中拼接时间字段，时间控制通过日期排序 + 页数限制实现。
    WoS: 使用 PY= (Publication Year) 限定。

    自动避免重复追加 PY= (多轮迭代时检索式可能已含时间限定)。
    """
    from datetime import datetime

    if date_range == "all" or not date_range:
        return query

    if "PY=" in query:
        return query

    today = datetime.now()

    if platform == "cnki":
        return query

    elif platform == "wos":
        if date_range in ("recent_7d", "recent_30d"):
            return f"{query} AND PY={today.year}"
        elif date_range == "recent_year":
            last_year = today.year - 1
            return f"{query} AND PY=({last_year} OR {today.year})"
        elif "-" in date_range:
            parts = date_range.split("-")
            return f"{query} AND PY=({parts[0]}-{parts[1]})"

    return query
