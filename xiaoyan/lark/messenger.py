"""
xiaoyan.lark.messenger — 飞书消息发送层
==========================================
封装 lark-cli im +messages-send / +messages-reply，
提供 Python 异步接口。

所有消息通过 lark-cli 子进程发送，不包含任何 Go/Node.js 代码。

参考:
  - lark-im-messages-send.md
  - lark-im-messages-reply.md
"""

import asyncio
import json
import logging
import shutil

logger = logging.getLogger("xiaoyan.lark.messenger")


async def _run_lark_cli(*args: str) -> tuple[int, str, str]:
    """执行 lark-cli 命令并返回 (returncode, stdout, stderr)"""
    lark_cli = shutil.which("lark-cli")
    if not lark_cli:
        raise RuntimeError("lark-cli not found in PATH")

    proc = await asyncio.create_subprocess_exec(
        lark_cli, *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return (
        proc.returncode,
        stdout.decode("utf-8").strip(),
        stderr.decode("utf-8").strip(),
    )


async def send_text(chat_id: str, text: str) -> dict | None:
    """
    向指定聊天发送纯文本消息。

    Args:
        chat_id: 飞书聊天 ID (oc_xxx)
        text: 消息内容

    Returns:
        发送成功时返回 {"message_id": "om_xxx", ...}，失败返回 None
    """
    rc, stdout, stderr = await _run_lark_cli(
        "im", "+messages-send",
        "--chat-id", chat_id,
        "--text", text,
        "--as", "bot",
    )

    if rc != 0:
        logger.error(f"发送文本消息失败: {stderr}")
        return None

    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        logger.warning(f"发送成功但返回值解析失败: {stdout}")
        return {"raw": stdout}


async def send_markdown(chat_id: str, markdown: str) -> dict | None:
    """
    向指定聊天发送 Markdown 格式消息。

    注意: lark-cli 会将 Markdown 转换为飞书 post 格式，
    不完全兼容 CommonMark。

    Args:
        chat_id: 飞书聊天 ID (oc_xxx)
        markdown: Markdown 内容

    Returns:
        发送结果
    """
    rc, stdout, stderr = await _run_lark_cli(
        "im", "+messages-send",
        "--chat-id", chat_id,
        "--markdown", markdown,
        "--as", "bot",
    )

    if rc != 0:
        logger.error(f"发送 Markdown 消息失败: {stderr}")
        return None

    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return {"raw": stdout}


async def send_card(chat_id: str, card_json: str) -> dict | None:
    """
    向指定聊天发送交互卡片消息。

    Args:
        chat_id: 飞书聊天 ID
        card_json: 飞书交互卡片 JSON 字符串

    Returns:
        发送结果
    """
    rc, stdout, stderr = await _run_lark_cli(
        "im", "+messages-send",
        "--chat-id", chat_id,
        "--msg-type", "interactive",
        "--content", card_json,
        "--as", "bot",
    )

    if rc != 0:
        logger.error(f"发送卡片消息失败: {stderr}")
        return None

    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return {"raw": stdout}


async def reply_text(message_id: str, text: str) -> dict | None:
    """
    回复指定消息（纯文本）。

    Args:
        message_id: 被回复消息的 ID (om_xxx)
        text: 回复内容

    Returns:
        回复结果
    """
    rc, stdout, stderr = await _run_lark_cli(
        "im", "+messages-reply",
        "--message-id", message_id,
        "--text", text,
        "--as", "bot",
    )

    if rc != 0:
        logger.error(f"回复消息失败: {stderr}")
        return None

    logger.info(f"回复已发送 (text, {len(text)}字) → {message_id[:20]}")
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return {"raw": stdout}


async def reply_markdown(message_id: str, markdown: str) -> dict | None:
    """
    回复指定消息（Markdown 格式）。

    Args:
        message_id: 被回复消息的 ID (om_xxx)
        markdown: Markdown 回复内容

    Returns:
        回复结果
    """
    rc, stdout, stderr = await _run_lark_cli(
        "im", "+messages-reply",
        "--message-id", message_id,
        "--markdown", markdown,
        "--as", "bot",
    )

    if rc != 0:
        logger.error(f"回复 Markdown 消息失败: {stderr}")
        return None

    logger.info(f"回复已发送 (markdown, {len(markdown)}字) → {message_id[:20]}")
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return {"raw": stdout}
