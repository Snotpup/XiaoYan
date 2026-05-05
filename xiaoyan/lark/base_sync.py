"""
xiaoyan.lark.base_sync — 飞书知识库 + 多维表格同步层 (P1-1)
=============================================================
将论文数据同步到飞书知识库内的 Bitable (多维表格)。

架构选型 (2026-04-04 实测确认):
  - 云盘 (Drive): bot 创建的 Base 用户看不到，需额外授权
  - 知识库 (Wiki): 左侧导航栏常驻可见，树形结构适合文献库
  - --as user: 数据归用户所有，天然可见 + 完全读写，零授权

  → 最终方案: 知识库 + --as user

职责:
  - 幂等创建知识库节点 (bitable) + 表 + 字段 (首次运行自动初始化)
  - 论文写入 (先查 data_hash 去重，再 record-upsert)
  - 被 scheduler._push_new_papers() 和
    adaptive_search._push_final() 调用

去重策略:
  lark-cli base +record-upsert 基于 --record-id 做更新，
  不支持按自定义字段自动去重。正确做法: 查 → 判 → 写。
"""

import asyncio
import json
import logging
import os
from datetime import datetime

from xiaoyan.lark.messenger import _run_lark_cli

logger = logging.getLogger("xiaoyan.lark.base_sync")

# 飞书 lark-cli 字段类型使用字符串枚举 (非数字)
# 支持: text, number, select, datetime, checkbox, link, ...
# 注意: "url" 不在枚举中，用 "text" 存链接即可 (飞书会自动渲染)
_TABLE_FIELDS = [
    {"name": "标题", "type": "text"},
    {"name": "期刊", "type": "text"},
    {"name": "摘要", "type": "text"},
    {"name": "来源平台", "type": "select", "options": [
        {"name": "CNKI"},
        {"name": "WoS"},
    ]},
    {"name": "AI 评述", "type": "text"},
    {"name": "爬取时间", "type": "datetime"},
    {"name": "数据哈希", "type": "text"},
    {"name": "详情链接", "type": "text"},
    {"name": "关键词", "type": "text"},
]

# lark-cli 统一身份 — 全部用 user 身份操作
_AS = "user"

# 模块级单例，跨调用复用 _existing_hashes 缓存
_base_manager: "LarkBaseManager | None" = None


def get_base_manager() -> "LarkBaseManager":
    """获取模块级单例 LarkBaseManager，确保 hash 缓存跨调用生效。"""
    global _base_manager
    if _base_manager is None:
        _base_manager = LarkBaseManager()
    return _base_manager


class LarkBaseManager:
    """飞书知识库多维表格管理器"""

    def __init__(self, base_token: str = "", table_id: str = ""):
        self._base_token = base_token or os.environ.get("XIAOYAN_BASE_TOKEN", "")
        self._table_id = table_id or os.environ.get("XIAOYAN_BASE_TABLE_ID", "")
        self._wiki_space_id = os.environ.get("XIAOYAN_WIKI_SPACE_ID", "")
        self._existing_hashes: set[str] | None = None  # 缓存

    @property
    def is_configured(self) -> bool:
        """是否已配置 Base"""
        return bool(self._base_token and self._table_id)

    async def ensure_base_exists(self) -> bool:
        """
        幂等初始化: 知识库 bitable 节点 → 表 → 字段。

        流程:
          1. 创建独立知识库空间 "XiaoYan 文献库" (与示例知识库平级)
          2. wiki nodes create (--as user) → 在空间内建 bitable 节点
          3. base +table-create (--as user)
          4. base +field-create ×9 (--as user, sleep 1s 防流控)
          5. 持久化到 .env

        全程 --as user，数据归用户所有，用户在知识库左侧导航栏直接可见。

        Returns:
            True = Base 可用，False = 创建失败或 API 不可用
        """
        if self.is_configured:
            return True

        logger.info("首次运行 Base 同步，在知识库中创建文献库...")

        try:
            # Step 1: 创建独立知识库空间 (与"示例知识库"平级)
            if not self._wiki_space_id:
                self._wiki_space_id = await self._create_wiki_space()
            if not self._wiki_space_id:
                logger.error("创建知识库空间失败")
                return False

            # Step 2: 在知识库空间内创建 bitable 节点
            rc, stdout, stderr = await _run_lark_cli(
                "wiki", "nodes", "create",
                "--params", json.dumps({
                    "space_id": self._wiki_space_id,
                }),
                "--data", json.dumps({
                    "obj_type": "bitable",
                    "node_type": "origin",
                    "title": "文献数据库",
                }),
                "--as", _AS,
            )
            if rc != 0:
                logger.error(f"创建知识库 bitable 节点失败: {stderr}")
                return False

            result = json.loads(stdout)
            node = result.get("data", {}).get("node", {})
            self._base_token = node.get("obj_token", "")

            if not self._base_token:
                logger.error(f"创建节点成功但未返回 obj_token: {stdout[:200]}")
                return False

            logger.info(f"知识库 bitable 已创建: {self._base_token}")

            # Step 3: 创建数据表
            rc, stdout, stderr = await _run_lark_cli(
                "base", "+table-create",
                "--base-token", self._base_token,
                "--name", "文献库",
                "--as", _AS,
            )
            if rc != 0:
                logger.error(f"创建表失败: {stderr}")
                return False

            result = json.loads(stdout)
            self._table_id = (
                result.get("data", {}).get("table", {}).get("id", "")
                or result.get("table_id", "")
                or result.get("data", {}).get("table_id", "")
            )

            if not self._table_id:
                logger.error(f"创建表成功但未返回 table_id: {stdout[:200]}")
                return False

            logger.info(f"表已创建: {self._table_id}，正在缓步添加字段...")

            # Step 4: 逐字段创建 (sleep 1s 防 OpenAPIAddField 限流)
            for field in _TABLE_FIELDS:
                frc, fout, ferr = await _run_lark_cli(
                    "base", "+field-create",
                    "--base-token", self._base_token,
                    "--table-id", self._table_id,
                    "--json", json.dumps(field, ensure_ascii=False),
                    "--as", _AS,
                )
                if frc != 0:
                    logger.warning(f"字段 {field['name']} 创建失败: {ferr[:120]}")
                await asyncio.sleep(1)

            logger.info("字段创建完毕")

            # Step 5: 清理 wiki 自动生成的默认空表
            await self._cleanup_default_table()

            # Step 6: 持久化到 .env
            _append_env("XIAOYAN_BASE_TOKEN", self._base_token)
            _append_env("XIAOYAN_BASE_TABLE_ID", self._table_id)
            _append_env("XIAOYAN_WIKI_SPACE_ID", self._wiki_space_id)

            return True

        except Exception as e:
            logger.error(f"Base 初始化失败: {e}", exc_info=True)
            return False

    async def _create_wiki_space(self) -> str:
        """
        创建独立的知识库空间 "XiaoYan 文献库"。
        使用 lark-cli api 通用接口调用 POST /wiki/v2/spaces。
        """
        try:
            rc, stdout, stderr = await _run_lark_cli(
                "api", "POST", "/open-apis/wiki/v2/spaces",
                "--data", json.dumps({
                    "name": "XiaoYan 文献库",
                    "description": "小研自动同步的学术文献知识库",
                }),
                "--as", _AS,
            )
            if rc != 0 or not stdout:
                logger.error(f"创建知识库空间失败: {stderr}")
                return ""

            data = json.loads(stdout)
            space = data.get("data", {}).get("space", {})
            space_id = space.get("space_id", "")
            if space_id:
                logger.info(f"知识库空间已创建: {space.get('name', '')} ({space_id})")
            return space_id

        except Exception as e:
            logger.error(f"创建知识库空间异常: {e}")
            return ""

    async def _cleanup_default_table(self):
        """
        wiki 创建 bitable 节点时会自动生成一个默认空表 (通常叫 Table 或数据表)。
        我们已经创建了自己的「文献库」表，需要删掉默认表以避免用户看到英文废表。
        """
        try:
            rc, stdout, _ = await _run_lark_cli(
                "base", "+table-list",
                "--base-token", self._base_token,
                "--as", _AS,
            )
            if rc != 0 or not stdout:
                return

            data = json.loads(stdout)
            items = data.get("data", {}).get("items", [])

            for item in items:
                tid = item.get("table_id", "")
                if tid and tid != self._table_id:
                    await _run_lark_cli(
                        "base", "+table-delete",
                        "--base-token", self._base_token,
                        "--table-id", tid,
                        "--yes",
                        "--as", _AS,
                    )
                    logger.info(f"已清理默认表: {item.get('table_name', tid)}")

        except Exception as e:
            logger.warning(f"清理默认表时出错 (不影响功能): {e}")

    async def sync_papers(self, papers: list[dict]) -> int:
        """
        同步论文到飞书 Base，先查 data_hash 去重。

        Args:
            papers: 论文列表，每项需要 title/journal/abstract/data_hash

        Returns:
            实际新增论文数
        """
        if not self.is_configured:
            ok = await self.ensure_base_exists()
            if not ok:
                logger.warning("Base 不可用，跳过同步")
                return 0

        # 拉取已有 hash 集合
        existing = await self._get_existing_hashes()

        # 过滤新论文
        new_papers = [
            p for p in papers
            if p.get("data_hash") and p["data_hash"] not in existing
        ]

        if not new_papers:
            logger.info("Base 同步: 无新增论文")
            return 0

        # 逐条插入
        success_count = 0
        for paper in new_papers:
            try:
                fields = self._paper_to_fields(paper)
                rc, stdout, stderr = await _run_lark_cli(
                    "base", "+record-upsert",
                    "--base-token", self._base_token,
                    "--table-id", self._table_id,
                    "--json", json.dumps(
                        fields, ensure_ascii=False
                    ),
                    "--as", _AS,
                )
                if rc == 0:
                    success_count += 1
                    existing.add(paper["data_hash"])
                else:
                    logger.warning(
                        f"Base 写入失败: {paper['title'][:30]} | {stderr[:100]}"
                    )
            except Exception as e:
                logger.warning(f"Base 写入异常: {e}")

        logger.info(f"Base 同步: 新增 {success_count}/{len(new_papers)} 篇")
        return success_count

    async def _get_existing_hashes(self) -> set[str]:
        """获取表中已有的 data_hash 集合 (分页拉取)"""
        if self._existing_hashes is not None:
            return self._existing_hashes

        hashes = set()
        offset = 0
        page_size = 100

        try:
            while True:
                rc, stdout, stderr = await _run_lark_cli(
                    "base", "+record-list",
                    "--base-token", self._base_token,
                    "--table-id", self._table_id,
                    "--limit", str(page_size),
                    "--offset", str(offset),
                    "--as", _AS,
                )
                if rc != 0 or not stdout:
                    break

                data = json.loads(stdout)
                payload = data.get("data", {})
                fields_arr = payload.get("fields", [])

                try:
                    hash_index = fields_arr.index("数据哈希")
                except ValueError:
                    hash_index = -1

                rows = payload.get("data", [])
                if not rows:
                    break

                if hash_index >= 0:
                    for row in rows:
                        if len(row) > hash_index:
                            h = row[hash_index]
                            if h:
                                hashes.add(h)

                if payload.get("has_more") is False:
                    break

                # 不足一页说明已到末尾
                if len(rows) < page_size:
                    break

                offset += page_size

        except Exception as e:
            logger.warning(f"获取已有 hash 失败: {e}")

        self._existing_hashes = hashes
        logger.info(f"Base 已有 {len(hashes)} 条记录")
        return hashes

    def _paper_to_fields(self, paper: dict) -> dict:
        """将论文字典转为飞书 Base 字段格式"""
        platform = paper.get("platform", "")
        if not platform:
            # 从标题推断
            title = paper.get("title", "")
            if any('\u4e00' <= c <= '\u9fff' for c in title):
                platform = "CNKI"
            else:
                platform = "WoS"

        fields = {
            "标题": paper.get("title", "")[:500],
            "期刊": paper.get("journal", ""),
            "摘要": (paper.get("abstract", "") or "")[:500],
            "来源平台": _normalize_platform_label(platform),
            "数据哈希": paper.get("data_hash", ""),
            "爬取时间": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }

        if paper.get("llm_comment"):
            fields["AI 评述"] = paper["llm_comment"]

        if paper.get("url"):
            fields["详情链接"] = paper["url"]

        if paper.get("keywords"):
            fields["关键词"] = paper["keywords"]

        return fields


def _normalize_platform_label(platform: str) -> str:
    """Return the exact select option label configured in the Lark Base."""
    normalized = (platform or "").strip().lower()
    if normalized == "cnki":
        return "CNKI"
    if normalized == "wos":
        return "WoS"
    return platform or ""


def _append_env(key: str, value: str):
    """追加环境变量到项目 .env 文件"""
    from pathlib import Path
    env_path = Path(__file__).parent.parent.parent / ".env"
    try:
        if env_path.exists():
            content = env_path.read_text(encoding="utf-8")
            if f"{key}=" in content:
                # 已存在则更新
                lines = content.split("\n")
                for i, line in enumerate(lines):
                    if line.startswith(f"{key}="):
                        lines[i] = f"{key}={value}"
                env_path.write_text("\n".join(lines), encoding="utf-8")
                return
        # 追加
        with open(env_path, "a", encoding="utf-8") as f:
            f.write(f"\n{key}={value}\n")
        logger.info(f"已写入 .env: {key}={value[:20]}...")
    except Exception as e:
        logger.warning(f"写入 .env 失败: {e}")
