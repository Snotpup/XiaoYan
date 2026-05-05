"""
config.py — 集中式配置中心
===========================
所有可调节参数统一管理，避免硬编码散落各处。
支持 Mac / Windows 双平台自适应。

迁移自 Academic_Scraper_Ultimate，路径改为基于包目录解析。
"""

import os
import platform

# ============================================================
#  平台自适应
# ============================================================
IS_MAC = platform.system() == "Darwin"
IS_WINDOWS = platform.system() == "Windows"

CHROME_PATH = (
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if IS_MAC
    else r"C:\Program Files\Google\Chrome\Application\chrome.exe"
)

# 键盘粘贴修饰键
PASTE_MODIFIER = "Meta" if IS_MAC else "Control"

# ============================================================
#  数据目录 (基于用户 home 目录，避免写到包内)
# ============================================================
_DATA_DIR = os.environ.get(
    "XIAOYAN_SCRAPER_DATA_DIR",
    os.path.join(os.path.expanduser("~"), ".xiaoyan", "scraper"),
)
os.makedirs(_DATA_DIR, exist_ok=True)

# ============================================================
#  浏览器持久化目录 (Cookie / Session 复用)
# ============================================================
CNKI_STATE_DIR = os.path.join(_DATA_DIR, "cnki_chrome_state")
WOS_STATE_DIR = os.path.join(_DATA_DIR, "wos_chrome_state")

# ============================================================
#  数据库
# ============================================================
PAPERS_DB_PATH = os.path.join(_DATA_DIR, "papers.db")
TASKS_DB_PATH = os.path.join(_DATA_DIR, "tasks.db")

# ============================================================
#  超时与延迟 (单位: 毫秒, 除非特别标注)
# ============================================================

# 人类模拟延迟范围 (毫秒)
HUMAN_DELAY_MIN = 800
HUMAN_DELAY_MAX = 2500

# 页面导航超时
NAVIGATION_TIMEOUT = 60_000

# 通用元素等待超时
DEFAULT_TIMEOUT = 30_000

# 人工验证码接管窗口 (当遇到 Cloudflare / 知网滑块时留给人手动操作的时间)
HUMAN_VERIFICATION_TIMEOUT = 120_000  # 2 分钟

# 翻页间隔
PAGE_TURN_DELAY_MIN = 2000
PAGE_TURN_DELAY_MAX = 4000

# ============================================================
#  安全限制 (防止宽泛检索导致长时间运行被封)
# ============================================================

# CNKI 默认最大翻页数 (每页 20 条, 3 页 = 60 条)
# CLI 手动指定 --pages 可覆盖; 传 0 或 -1 表示不限
CNKI_DEFAULT_MAX_PAGES = 3

# CNKI 详情页并发获取 (迁移自智研TRACK v2.1 多线程思路, 改为 asyncio 实现)
CNKI_DETAIL_CONCURRENCY = 3   # 同时打开的详情页标签数 (建议 3-5, 过高易触发反爬)
CNKI_DETAIL_DELAY_MS = 500    # 每个详情页获取完毕后的冷却延迟 (ms)

# WoS 快速模式默认最大提取条数
WOS_DEFAULT_MAX_RECORDS = 20

# 单次爬取全局超时 (秒) — 超过此时间强制终止，防止挂死
SCRAPE_GLOBAL_TIMEOUT = 300  # 5 分钟

# ============================================================
#  CNKI 专属配置
# ============================================================
CNKI_SEARCH_URL = "https://kns.cnki.net/kns8s/AdvSearch?classid=YSTT4HG0"

# 来源类别映射 (key: 知网 checkbox key, value: 显示名)
CNKI_SOURCE_MAP = {
    "SI":  "SCI来源期刊",
    "EI":  "EI来源期刊",
    "HX":  "北大核心",
    "CSI": "CSSCI",
    "CSD": "CSCD",
    "AMI": "AMI",
}

# ============================================================
#  WoS 专属配置
# ============================================================
WOS_SEARCH_URL = "https://webofscience.clarivate.cn/wos/woscc/advanced-search"
WOS_INTL_SEARCH_URL = "https://www.webofscience.com/wos/woscc/advanced-search"

# BibTeX 导出单次上限
WOS_MAX_EXPORT_PER_FILE = 500

# BibTeX 下载存放目录
WOS_DOWNLOAD_DIR = os.path.join(_DATA_DIR, "wos_downloads")

# BibTeX 预期字段列表 (对应 WoS Full Record and Cited References)
WOS_BIBTEX_FIELDS = [
    "Author", "Title", "Journal", "Year", "Volume", "Number", "Pages", "Month",
    "Abstract", "Publisher", "Address", "Type", "Language", "Affiliation", "DOI",
    "EarlyAccessDate", "ISSN", "EISSN", "Keywords", "Keywords-Plus",
    "Research-Areas", "Web-of-Science-Categories", "Author-Email", "Affiliations",
    "ResearcherID-Numbers", "ORCID-Numbers", "Funding-Acknowledgement", "Funding-Text",
    "Cited-References", "Number-of-Cited-References", "Times-Cited",
    "Usage-Count-Last-180-days", "Usage-Count-Since-2013", "Journal-ISO",
    "Doc-Delivery-Number", "Web-of-Science-Index", "Unique-ID",
    "Article-Number", "OA", "DA",
]

# ============================================================
#  期刊分区数据 (迁移自智研TRACK 分区查询.py)
# ============================================================
JCR_DB_PATH = os.environ.get(
    "XIAOYAN_JCR_DB_PATH",
    os.path.join(_DATA_DIR, "jcr.db"),
)
JCR_TABLE_NAME = "FQBJCR2025"          # JCR 分区表名
JCR_JOURNAL_COL = "Journal"            # JCR 表中的期刊列名
JCR_DALEI_COL = "大类"                 # 中科院大类列名
JCR_FENQU_COL = "大类分区"             # 中科院分区列名

# ============================================================
#  浏览器指纹
# ============================================================
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

VIEWPORT = {"width": 1440, "height": 900}
