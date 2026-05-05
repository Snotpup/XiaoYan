"""
xiaoyan.config — 统一配置管理
================================
从 .env 文件和环境变量读取配置，提供全局默认值。
所有配置集中管理，其他模块通过 import 使用。

设计原则:
  - 用户不需要手动编辑此文件
  - 所有可配置项通过 .env 或环境变量设置
  - 合理的默认值让项目能最小配置起步
"""

import os
import time
from pathlib import Path

from dotenv import load_dotenv

# 默认设置中国时区 (避免跨平台/如 Docker 部署时 SQLite datetime 不对)
if "TZ" not in os.environ:
    os.environ["TZ"] = "Asia/Shanghai"
    if hasattr(time, "tzset"):
        time.tzset()

# ============================================================
#  加载 .env
# ============================================================

# 优先从项目根目录加载，再从用户 home 目录加载
_project_root = Path(__file__).parent.parent
_env_file = _project_root / ".env"
if _env_file.exists():
    load_dotenv(_env_file)
else:
    # 备选: 用户 home 目录下的 .xiaoyan/.env
    _home_env = Path.home() / ".xiaoyan" / ".env"
    if _home_env.exists():
        load_dotenv(_home_env)


# ============================================================
#  数据目录
# ============================================================

DATA_DIR = Path(
    os.environ.get("XIAOYAN_DATA_DIR", Path.home() / ".xiaoyan")
)
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "xiaoyan.db"


# ============================================================
#  LLM 配置
# ============================================================

LLM_API_KEY = os.environ.get("XIAOYAN_LLM_API_KEY", "")
LLM_BASE_URL = os.environ.get("XIAOYAN_LLM_BASE_URL", "https://api.openai.com/v1")
LLM_MODEL = os.environ.get("XIAOYAN_LLM_MODEL", "gpt-4o-mini")


# ============================================================
#  飞书配置
# ============================================================

LARK_CHAT_ID = os.environ.get("XIAOYAN_LARK_CHAT_ID", "")

# P1-1: 飞书知识库 + 多维表格配置 (首次运行自动创建并写入)
WIKI_SPACE_ID = os.environ.get("XIAOYAN_WIKI_SPACE_ID", "")
BASE_TOKEN = os.environ.get("XIAOYAN_BASE_TOKEN", "")
BASE_TABLE_ID = os.environ.get("XIAOYAN_BASE_TABLE_ID", "")


# ============================================================
#  调度配置
# ============================================================

DEFAULT_CRON = os.environ.get("XIAOYAN_DEFAULT_CRON", "0 */6 * * *")

# 爬取参数 (可通过 .env 覆盖)
WOS_MAX_RECORDS = int(os.environ.get("XIAOYAN_WOS_MAX_RECORDS", "20"))
WOS_SITE = os.environ.get("XIAOYAN_WOS_SITE", "com")
CNKI_MAX_PAGES = int(os.environ.get("XIAOYAN_CNKI_MAX_PAGES", "3"))


# ============================================================
#  验证辅助
# ============================================================

def validate_config() -> list[str]:
    """检查关键配置是否已设置，返回缺失项列表。"""
    missing = []
    if not LLM_API_KEY or LLM_API_KEY.startswith("sk-your"):
        missing.append("XIAOYAN_LLM_API_KEY")
    if not LARK_CHAT_ID or LARK_CHAT_ID.startswith("oc_your"):
        missing.append("XIAOYAN_LARK_CHAT_ID")
    return missing
