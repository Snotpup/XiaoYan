"""
xiaoyan.lark.doc_writer — 飞书文档创建/更新层 (P1-3)
=====================================================
封装 lark-cli docs +create / +update，提供 Python 异步接口。
复用 messenger.py 的 _run_lark_cli 模式。

共用方:
  - report.py         (P1-3 文献分析报告)
  - adaptive_search.py (深度探索报告升级)
  - 未来 P2-2 研究周报

参考: [P1-3-report-generation.md] 第三节
"""

import json
import logging

from xiaoyan.lark.messenger import _run_lark_cli

logger = logging.getLogger("xiaoyan.lark.doc_writer")


class LarkDocWriter:
    """飞书文档写入器"""

    async def create_doc(
        self,
        title: str,
        markdown: str,
        wiki_space: str = "",
        folder_token: str = "",
    ) -> dict:
        """
        创建飞书文档。

        Args:
            title: 文档标题
            markdown: Markdown 内容 (飞书原生支持)
            wiki_space: 知识库空间 ID，"my_library" = 个人知识库
            folder_token: 可选，指定云空间文件夹

        Returns:
            {"doc_token": "...", "url": "https://...feishu.cn/docx/..."}
        """
        args = [
            "docs", "+create",
            "--title", title,
            "--markdown", markdown,
            "--as", "bot",
        ]
        if wiki_space:
            args.extend(["--wiki-space", wiki_space])
        if folder_token:
            args.extend(["--folder-token", folder_token])

        rc, stdout, stderr = await _run_lark_cli(*args)
        if rc != 0:
            raise RuntimeError(f"创建飞书文档失败: {stderr}")

        try:
            return _normalize_create_doc_response(json.loads(stdout))
        except json.JSONDecodeError:
            # 返回原始输出，至少保留 url 信息
            logger.warning(f"飞书文档响应解析失败: {stdout[:200]}")
            return {"raw": stdout, "url": ""}

    async def append_to_doc(
        self,
        doc: str,
        markdown: str,
    ) -> bool:
        """
        向已有文档追加内容。

        Args:
            doc: 文档 URL 或 token
            markdown: 追加的 Markdown 内容

        Returns:
            是否成功
        """
        rc, stdout, stderr = await _run_lark_cli(
            "docs", "+update",
            "--doc", doc,
            "--markdown", markdown,
            "--mode", "append",
            "--as", "bot",
        )
        if rc != 0:
            logger.error(f"飞书文档追加失败: {stderr}")
            return False
        return True


def _normalize_create_doc_response(payload: dict) -> dict:
    """兼容 lark-cli 不同版本的文档创建返回结构。"""
    data = payload.get("data")
    if isinstance(data, dict):
        url = payload.get("url") or data.get("doc_url") or data.get("url") or ""
        token = (
            payload.get("doc_token")
            or data.get("doc_token")
            or data.get("doc_id")
            or ""
        )
        if url:
            payload["url"] = url
        if token:
            payload["doc_token"] = token
    return payload
