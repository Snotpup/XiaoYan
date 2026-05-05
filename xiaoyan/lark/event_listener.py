"""
xiaoyan.lark.event_listener — 飞书事件监听器
==============================================
管理 lark-cli event +subscribe 子进程，
持续读取 NDJSON 事件流并转化为 Python 事件。

核心设计:
  - 启动 lark-cli event +subscribe 作为子进程
  - 使用 --compact --quiet 获取扁平化 NDJSON
  - 异步逐行读取 stdout
  - 解析 JSON 并通过回调分发

参考: lark-event-subscribe.md — Agent Pipeline 示例
"""

import asyncio
import json
import logging
import shutil
from typing import Callable, Awaitable

logger = logging.getLogger("xiaoyan.lark.event")

# 事件回调类型: async def handler(event: dict) -> None
EventHandler = Callable[[dict], Awaitable[None]]


class LarkEventListener:
    """
    飞书事件监听器。

    用法:
        listener = LarkEventListener()
        listener.on_message(handle_message)
        await listener.start()
    """

    def __init__(self, event_types: str = "im.message.receive_v1"):
        self.event_types = event_types
        self._handlers: list[EventHandler] = []
        self._process: asyncio.subprocess.Process | None = None
        self._running = False

    def on_message(self, handler: EventHandler):
        """注册消息事件处理函数"""
        self._handlers.append(handler)

    async def start(self):
        """启动事件监听子进程 (含自动重连)"""
        self._running = True
        max_retries = 10
        retry_count = 0

        while self._running:
            try:
                start_time = asyncio.get_event_loop().time()
                await self._start_subprocess()
                if not self._running:
                    break

                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed > 60:
                    # 运行超过 1 分钟后崩溃，重置重试计数
                    retry_count = 0
                else:
                    retry_count += 1

                if retry_count > max_retries:
                    logger.error(
                        f"lark-cli 子进程已连续崩溃 {max_retries} 次，停止重连"
                    )
                    break

                delay = min(2 ** retry_count, 60)
                logger.warning(
                    f"lark-cli 子进程退出 (运行 {elapsed:.0f}s)，{delay}s 后重连 "
                    f"(第 {retry_count}/{max_retries} 次)"
                )
                await asyncio.sleep(delay)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"事件监听启动失败: {e}", exc_info=True)
                retry_count += 1
                delay = min(2 ** retry_count, 60)
                await asyncio.sleep(delay)

    async def _start_subprocess(self):
        """启动 lark-cli 子进程并等待其退出"""
        lark_cli = shutil.which("lark-cli")
        if not lark_cli:
            logger.error(
                "lark-cli 未找到！请先安装: npm install -g @larksuite/cli"
            )
            raise RuntimeError("lark-cli not found in PATH")

        cmd = [
            lark_cli, "event", "+subscribe",
            "--event-types", self.event_types,
            "--compact", "--quiet",
            "--as", "bot",
        ]

        logger.info(f"启动事件监听: {' '.join(cmd)}")

        self._process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # 并行读取 stdout 和 stderr
        await asyncio.gather(
            self._read_events(),
            self._read_stderr(),
        )

    async def _read_events(self):
        """逐行读取 NDJSON 事件流"""
        assert self._process and self._process.stdout

        while self._running:
            try:
                line = await self._process.stdout.readline()
                if not line:
                    # 子进程退出
                    logger.warning("lark-cli event 子进程 stdout 关闭")
                    break

                line_str = line.decode("utf-8").strip()
                if not line_str:
                    continue

                try:
                    event = json.loads(line_str)
                except json.JSONDecodeError as e:
                    logger.warning(f"JSON 解析失败: {e} | 原始行: {line_str[:200]}")
                    continue

                # 分发给所有注册的 handler
                for handler in self._handlers:
                    try:
                        await handler(event)
                    except Exception as e:
                        logger.error(f"事件处理异常: {e}", exc_info=True)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"事件读取异常: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def _read_stderr(self):
        """读取 stderr 并记录日志 (非 quiet 时会有状态输出)"""
        assert self._process and self._process.stderr

        while self._running:
            try:
                line = await self._process.stderr.readline()
                if not line:
                    break
                msg = line.decode("utf-8").strip()
                if msg:
                    logger.debug(f"[lark-cli stderr] {msg}")
            except asyncio.CancelledError:
                break
            except Exception:
                break

    async def stop(self):
        """优雅停止事件监听"""
        self._running = False
        if self._process:
            try:
                self._process.terminate()
                await asyncio.wait_for(self._process.wait(), timeout=5)
            except (asyncio.TimeoutError, ProcessLookupError):
                try:
                    self._process.kill()
                except ProcessLookupError:
                    pass  # 进程已退出
            logger.info("事件监听已停止")

    @property
    def is_running(self) -> bool:
        return self._running and self._process is not None


def parse_message_event(event: dict) -> dict | None:
    """
    解析 compact 格式的 IM 消息事件，提取关键字段。

    compact 格式示例:
    {
        "type": "im.message.receive_v1",
        "message_id": "om_xxx",
        "chat_id": "oc_xxx",
        "chat_type": "p2p",
        "message_type": "text",
        "content": "帮我查一下 AI 医疗的文献",
        "sender_id": "ou_xxx",
        "create_time": "1773491924409"
    }
    """
    event_type = event.get("type", "")
    if event_type != "im.message.receive_v1":
        return None

    content = event.get("content", "")
    if not content:
        return None

    return {
        "message_id": event.get("message_id", ""),
        "chat_id": event.get("chat_id", ""),
        "chat_type": event.get("chat_type", ""),
        "message_type": event.get("message_type", ""),
        "content": content,
        "sender_id": event.get("sender_id", ""),
        "sender_type": event.get("sender_type", ""),
        "timestamp": event.get("create_time", ""),
    }
