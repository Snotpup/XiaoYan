"""
xiaoyan.core.engine — 核心编排引擎
=====================================
XiaoYan 的大脑。串联事件监听、意图识别、爬取调度和消息回复。

架构:
  ┌─────────────────┐
  │ LarkEventListener│──→ on_message ──→ handle_message()
  └─────────────────┘                        │
       ↑                                     ↓
  lark-cli event            ┌──────────────────────────────┐
  +subscribe subprocess     │     Intent Recognition       │
                            │     (LLM JSON output)        │
                            └──────────────┬───────────────┘
                                           │
              ┌────────────────────────────┼──────────────────────┐
              ↓                            ↓                     ↓
        search handler            query CRUD handler       unknown handler
        (scrape + push)           (DB + confirm card)      (polite reply)
"""

import asyncio
from collections import deque
import logging
import re
import signal
import sys

from xiaoyan.config import LARK_CHAT_ID, validate_config
from xiaoyan.core.db import (
    init_db,
    add_search_query,
    update_search_query,
    delete_search_query,
    list_search_queries,
    get_setting,
    set_setting,
    get_all_settings,
)
from xiaoyan.core.intent import recognize_intent, generate_paper_comment
from xiaoyan.core.settings_flow import (
    SettingsFlow,
    SETTINGS_META,
    resolve_setting_display,
    get_meta_by_key,
    get_option_by_index,
)
from xiaoyan.core.utils import looks_chinese
from xiaoyan.core.memory import ConversationContext, SessionMemory
from xiaoyan.core.profile import ResearchProfile
from xiaoyan.core.scheduler import ScrapeScheduler
from xiaoyan.lark.event_listener import LarkEventListener, parse_message_event
from xiaoyan.lark.messenger import reply_text, reply_markdown, send_card
from xiaoyan.lark.card_builder import (
    build_query_list_card,
    build_confirm_card,
    build_paper_card,
    build_settings_card,
    build_setting_options_card,
    build_setting_confirm_card,
)

logger = logging.getLogger("xiaoyan.core.engine")


class XiaoYanEngine:
    """
    小研核心引擎。

    职责:
      1. 启动飞书事件监听 (lark-cli WebSocket 子进程)
      2. 启动定时爬取调度 (APScheduler)
      3. 接收消息 → 意图识别 → 分发处理 → 回复
    """

    def __init__(self):
        self._listener = LarkEventListener()
        self._scheduler = ScrapeScheduler()
        self._context = ConversationContext()
        self._session_memory = SessionMemory()
        self._profile = ResearchProfile()  # P1-2: 研究方向画像
        self._settings_flow = SettingsFlow()
        self._seen_message_ids = set()
        self._seen_message_order = deque(maxlen=200)
        self._running = False

    async def start(self):
        """启动引擎 (阻塞运行)"""
        # 预检
        missing = validate_config()
        if missing:
            logger.error(f"配置缺失: {', '.join(missing)}")
            logger.error("请运行 `xiaoyan init` 完成配置")
            sys.exit(1)

        # 初始化数据库
        init_db()

        # 注册消息处理器
        self._listener.on_message(self._handle_event)

        # 启动定时调度
        await self._scheduler.start()

        self._running = True
        logger.info("🚀 小研已启动，等待飞书消息...")

        # 注册信号处理
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

        # 启动事件监听 (阻塞)
        try:
            await self._listener.start()
        except Exception as e:
            logger.error(f"事件监听异常退出: {e}", exc_info=True)
        finally:
            await self.stop()

    async def stop(self):
        """优雅停止"""
        if not self._running:
            return
        self._running = False

        logger.info("正在停止小研...")
        await self._listener.stop()
        self._scheduler.shutdown()
        logger.info("小研已停止 👋")

    # ================================================================
    #  事件分发
    # ================================================================

    async def _handle_event(self, event: dict):
        """处理原始事件"""
        msg = parse_message_event(event)
        if not msg:
            return  # 非消息事件，忽略

        # 过滤 bot 自身消息，防止死循环
        sender_type = event.get("sender_type", "")
        if sender_type == "app":
            return

        # 只处理文本消息
        if msg["message_type"] != "text":
            await reply_text(msg["message_id"], "目前只支持文本消息哦 📝")
            return

        content = msg["content"]
        message_id = msg["message_id"]
        chat_id = msg["chat_id"]

        if message_id and self._is_duplicate_message(message_id):
            logger.info(f"忽略重复消息事件: {message_id}")
            return

        logger.info(f"收到消息: {content[:80]}")

        # 设置流程拦截: 如果用户正在设置菜单中，优先走 settings 处理
        if self._settings_flow.is_in_flow(chat_id):
            handled = await self._handle_settings_flow_input(
                message_id, chat_id, content,
            )
            if handled:
                return

        try:
            # 构建上下文摘要，注入意图识别
            recent_context = self._context.format_recent(n=6)
            memory_summary = self._session_memory.load_summary(max_len=600)

            # P1-2: 合并画像摘要到意图识别上下文
            profile_summary = self._profile.format_summary(max_len=200)
            combined_context = memory_summary
            if profile_summary:
                combined_context += f"\n\n## 用户研究画像\n{profile_summary}"

            # 意图识别 (带上下文)
            result = await recognize_intent(
                content,
                recent_context=recent_context,
                memory_summary=combined_context,
            )
            intent = result["intent"]
            params = result["params"]

            # 记录到滑动窗口
            self._context.add_user(content, intent=intent)

            # 分发到对应处理器
            handlers = {
                "search": self._handle_search,
                "deep_explore": self._handle_deep_explore,
                "add_query": self._handle_add_query,
                "update_query": self._handle_update_query,
                "delete_query": self._handle_delete_query,
                "list_queries": self._handle_list_queries,
                "show_profile": self._handle_show_profile,       # P1-2
                "generate_report": self._handle_generate_report,  # P1-3
                "settings": self._handle_settings,
                "chat": self._handle_chat,
                "unknown": self._handle_unknown,
            }

            handler = handlers.get(intent, self._handle_unknown)
            await handler(message_id, params, content)

            # 记录 bot 行为到上下文
            self._context.add_assistant(self._build_reply_summary(intent, params, content))

            # 异步触发会话记忆更新 (不阻塞回复)
            self._session_memory.tick()
            if self._session_memory.should_update():
                asyncio.create_task(self._session_memory.update(self._context))

        except Exception as e:
            logger.error(f"消息处理异常: {e}", exc_info=True)
            await reply_text(
                message_id,
                f"处理消息时出错了 😅\n错误: {str(e)[:100]}"
            )

    def _is_duplicate_message(self, message_id: str) -> bool:
        """基于飞书 message_id 防止同一事件被重复处理。"""
        if message_id in self._seen_message_ids:
            return True

        if len(self._seen_message_order) == self._seen_message_order.maxlen:
            oldest = self._seen_message_order.popleft()
            self._seen_message_ids.discard(oldest)

        self._seen_message_ids.add(message_id)
        self._seen_message_order.append(message_id)
        return False

    def _build_reply_summary(self, intent: str, params: dict, raw: str) -> str:
        """根据意图和参数生成 ≤50 字的回复摘要，用于上下文记忆。"""
        if intent == "search":
            topic = params.get("topic", raw)[:30]
            platform = params.get("platform", "")
            return f"[search] 检索 {platform}: {topic}"
        elif intent == "deep_explore":
            topic = params.get("topic", raw)[:30]
            return f"[deep_explore] 深度探索: {topic}"
        elif intent == "add_query":
            q = params.get("query", "")[:30]
            return f"[add_query] 添加检索式: {q}"
        elif intent == "update_query":
            qid = params.get("query_id", "?")
            return f"[update_query] 修改检索式 #{qid}"
        elif intent == "delete_query":
            qid = params.get("query_id", "?")
            return f"[delete_query] 删除检索式 #{qid}"
        elif intent == "list_queries":
            return "[list_queries] 展示检索式列表"
        elif intent == "show_profile":
            return "[show_profile] 展示研究画像"
        elif intent == "generate_report":
            topic = params.get("topic_filter", "")[:20]
            return f"[generate_report] 文献报告: {topic}" if topic else "[generate_report] 生成文献报告"
        elif intent == "chat":
            reply = params.get("reply", "")[:30]
            return f"[chat] {reply}" if reply else "[chat] 已回复"
        elif intent == "settings":
            return "[settings] 系统设置"
        return f"[{intent}] 已处理"

    # ================================================================
    #  意图处理器
    # ================================================================

    async def _handle_search(self, message_id: str, params: dict, raw: str):
        """处理临时检索请求 — 委托给 AdaptiveSearcher"""
        from xiaoyan.core.adaptive_search import AdaptiveSearcher

        topic = params.get("topic", raw)
        default_platform = "cnki" if looks_chinese(topic) else "wos"
        profile_summary = self._profile.format_summary()

        searcher = AdaptiveSearcher(
            scheduler=self._scheduler,
            message_id=message_id,
            profile_summary=profile_summary,
        )
        await searcher.search(
            topic=topic,
            query=params.get("query", ""),
            platform=params.get("platform", default_platform),
            date_range=params.get("date_range", "recent_30d"),
        )

    async def _handle_deep_explore(self, message_id: str, params: dict, raw: str):
        """处理深度探索请求 — 委托给 AdaptiveSearcher.deep_explore"""
        from xiaoyan.core.adaptive_search import AdaptiveSearcher

        topic = params.get("topic", raw)
        default_platform = "cnki" if looks_chinese(topic) else "wos"
        profile_summary = self._profile.format_summary()

        searcher = AdaptiveSearcher(
            scheduler=self._scheduler,
            message_id=message_id,
            profile_summary=profile_summary,
        )
        await searcher.deep_explore(
            topic=topic,
            platform=params.get("platform", default_platform),
        )

    async def _handle_add_query(self, message_id: str, params: dict, raw: str):
        """处理添加检索式请求"""
        query = params.get("query", "")
        platform = params.get("platform", "wos")
        description = params.get("description", "")
        default_cron = get_setting("default_cron") or "0 */6 * * *"
        cron = params.get("cron", default_cron)

        if not query:
            await reply_text(
                message_id,
                "请提供检索式内容。\n"
                "例如: 添加定时检索 TS=(AI AND healthcare)\n"
                "或: 帮我追踪 人工智能+医疗 的知网文献"
            )
            return

        query_id = add_search_query(
            platform=platform,
            query=query,
            description=description or raw[:50],
            cron_expression=cron,
        )

        # 重载调度器
        await self._scheduler.reload()

        detail = (
            f"**ID**: #{query_id}\n"
            f"**平台**: {platform.upper()}\n"
            f"**检索式**: `{query}`\n"
            f"**定时**: `{cron}`\n"
            f"**描述**: {description or raw[:50]}"
        )
        card_json = build_confirm_card("检索式已添加", detail)
        await send_card(LARK_CHAT_ID, card_json)

    async def _handle_update_query(self, message_id: str, params: dict, raw: str):
        """处理修改检索式请求"""
        query_id = params.get("query_id")
        if not query_id:
            await reply_text(
                message_id,
                "请指定要修改的检索式 ID。\n"
                "先发「看看我的检索式」查看列表。"
            )
            return

        update_fields = {
            k: v for k, v in params.items()
            if k in ("query", "platform", "description", "cron_expression", "is_active")
            and v is not None
        }

        if update_fields:
            success = update_search_query(int(query_id), **update_fields)
            if success:
                await self._scheduler.reload()
                await reply_text(message_id, f"✅ 检索式 #{query_id} 已更新")
            else:
                await reply_text(message_id, f"未找到 ID 为 {query_id} 的检索式")
        else:
            await reply_text(message_id, "请说明要修改什么内容。")

    async def _handle_delete_query(self, message_id: str, params: dict, raw: str):
        """处理删除检索式请求"""
        query_id = params.get("query_id")
        if not query_id:
            await reply_text(
                message_id,
                "请指定要删除的检索式 ID。\n"
                "先发「看看我的检索式」查看列表。"
            )
            return

        success = delete_search_query(int(query_id))
        if success:
            await self._scheduler.reload()
            await reply_text(message_id, f"✅ 检索式 #{query_id} 已删除")
        else:
            await reply_text(message_id, f"未找到 ID 为 {query_id} 的检索式")

    async def _handle_list_queries(self, message_id: str, params: dict, raw: str):
        """处理查看检索式列表请求"""
        queries = list_search_queries(active_only=False)
        card_json = build_query_list_card(queries)
        await send_card(LARK_CHAT_ID, card_json)

    async def _handle_chat(self, message_id: str, params: dict, raw: str):
        """处理用户的闲聊与自然语言响应"""
        reply = params.get("reply", "我已经收到了你的消息！")
        await reply_text(message_id, reply)

        # P1-2: 从 profile_hint 中提取画像更新
        profile_hint = params.get("profile_hint")
        if profile_hint and isinstance(profile_hint, dict):
            try:
                self._profile.update(profile_hint)
                logger.info(f"画像已通过 chat 意图更新: {profile_hint}")
            except Exception as e:
                logger.warning(f"画像更新失败: {e}")

    async def _handle_show_profile(self, message_id: str, params: dict, raw: str):
        """处理查看研究画像请求 (P1-2)"""
        card_text = self._profile.format_card_text()
        await reply_markdown(message_id, card_text)

    async def _handle_generate_report(self, message_id: str, params: dict, raw: str):
        """处理文献报告生成请求 (P1-3)"""
        from xiaoyan.core.report import ReportGenerator
        from xiaoyan.lark.doc_writer import LarkDocWriter

        await reply_text(message_id, "📊 正在分析文献数据，报告生成中...")

        gen = ReportGenerator()
        source = params.get("source", "recent")
        report_type = params.get("report_type", "summary")
        topic = params.get("topic_filter", "")

        papers = await gen.fetch_papers(source)
        if not papers:
            await reply_text(message_id, "📭 没有找到可分析的文献，请先执行一次检索。")
            return

        report_md = await gen.generate(papers, report_type, topic)

        if len(report_md) < 800:
            # 短报告: 直接 IM 推送
            await reply_markdown(message_id, report_md)
        else:
            # 长报告: 创建飞书文档
            from datetime import datetime as _dt
            title = "文献分析报告"
            if topic:
                title += f" — {topic}"
            title += f" ({_dt.now().strftime('%m-%d')})"

            try:
                writer = LarkDocWriter()
                result = await writer.create_doc(
                    title=title,
                    markdown=report_md,
                    wiki_space="my_library",
                )
                doc_url = result.get("url", "")

                summary = (
                    f"📊 **文献分析报告已生成！**\n\n"
                    f"📄 [点击查看完整报告]({doc_url})\n\n"
                    f"共分析 **{len(papers)}** 篇文献"
                )
                await reply_markdown(message_id, summary)

            except Exception as e:
                logger.warning(f"飞书文档创建失败，降级为 IM 推送: {e}")
                preview = report_md[:1500] + "\n\n---\n⚠ 报告较长，但飞书文档创建失败"
                await reply_markdown(message_id, preview)

    # ================================================================
    #  设置菜单
    # ================================================================

    async def _handle_settings(self, message_id: str, params: dict, raw: str):
        """处理 settings 意图 — 展示设置卡片"""
        settings = get_all_settings()
        card_json = build_settings_card(settings, SETTINGS_META, resolve_setting_display)
        chat_id = LARK_CHAT_ID
        await send_card(chat_id, card_json)

        # 如果意图中直接指定了 setting_key，跳到选项卡片
        setting_key = params.get("setting_key")
        if setting_key and get_meta_by_key(setting_key):
            meta = get_meta_by_key(setting_key)
            current = settings.get(setting_key, "")
            options_card = build_setting_options_card(
                setting_key, meta["label"], current,
                meta["options"], resolve_setting_display,
            )
            await send_card(chat_id, options_card)
            self._settings_flow.set_choose_value(chat_id, setting_key)
        else:
            # 进入选择设置项流程，等待用户回复编号
            self._settings_flow.set_choose_setting(chat_id)

    async def _handle_settings_flow_input(
        self, message_id: str, chat_id: str, content: str,
    ) -> bool:
        """
        处理设置流程中的文本输入 (编号选择)。
        返回 True 表示已消费此消息，False 表示应继续走正常意图识别。
        """
        state = self._settings_flow.get_state(chat_id)
        if not state:
            return False

        text = content.strip()

        # 取消
        if SettingsFlow.is_cancel(text):
            self._settings_flow.clear(chat_id)
            await reply_text(message_id, "已退出设置。")
            return True

        step = state.get("step")

        if step == "choose_setting":
            # 用户选择要修改哪个设置项
            try:
                index = int(text)
            except ValueError:
                await reply_text(
                    message_id,
                    f"请输入编号选择要修改的设置项，或回复「取消」退出。\n"
                    f"可选编号: 1~{len(SETTINGS_META)}",
                )
                return True

            from xiaoyan.core.settings_flow import get_meta_by_index
            meta = get_meta_by_index(index)
            if not meta:
                await reply_text(
                    message_id,
                    f"编号无效，可选范围: 1~{len(SETTINGS_META)}",
                )
                return True

            key = meta["key"]
            settings = get_all_settings()
            current = settings.get(key, "")
            options_card = build_setting_options_card(
                key, meta["label"], current,
                meta["options"], resolve_setting_display,
            )
            await send_card(chat_id, options_card)
            self._settings_flow.set_choose_value(chat_id, key)
            return True

        if step == "choose_value":
            key = state.get("key", "")
            meta = get_meta_by_key(key)
            if not meta:
                self._settings_flow.clear(chat_id)
                return True

            # 解析编号
            try:
                index = int(text)
            except ValueError:
                await reply_text(
                    message_id,
                    f"请输入编号选择，或回复「取消」退出。\n"
                    f"可选编号: 1~{len(meta['options'])}",
                )
                return True

            option = get_option_by_index(key, index)
            if not option:
                await reply_text(
                    message_id,
                    f"编号无效，可选范围: 1~{len(meta['options'])}",
                )
                return True

            label, new_value = option
            set_setting(key, new_value)
            self._settings_flow.clear(chat_id)

            # 热重载调度器
            await self._scheduler.reload()

            display = resolve_setting_display(key, new_value)
            confirm_card = build_setting_confirm_card(meta["label"], display)
            await send_card(chat_id, confirm_card)
            logger.info(f"设置已更新: {key} = {new_value}")
            return True

        # 未知步骤，清除状态
        self._settings_flow.clear(chat_id)
        return False

    async def _handle_unknown(self, message_id: str, params: dict, raw: str):
        """处理完全无法识别的意图（帮助菜单降级）"""
        await reply_markdown(
            message_id,
            "你好！我是小研 🧑‍🔬 你的科研助理。\n\n"
            "我能帮你做这些事：\n"
            "- 🔍 **临时检索**: \"帮我查一下 AI 医疗的文献\"\n"
            "- ➕ **添加定时检索**: \"帮我追踪 深度学习+药物发现\"\n"
            "- 📋 **查看检索式**: \"看看我的检索式\"\n"
            "- ✏️ **修改检索式**: \"把 #1 的检索式改成 xxx\"\n"
            "- 🗑️ **删除检索式**: \"删掉 #2\"\n"
            "- 📋 **查看画像**: \"看看我的画像\"\n"
            "- 📊 **文献报告**: \"总结最近的文献\"\n"
            "- ⚙️ **系统设置**: \"设置\"\n\n"
            "试试发一条指令吧！"
        )
