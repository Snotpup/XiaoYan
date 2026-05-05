"""
xiaoyan.scraper — 学术文献爬取子包
====================================
迁移自 Academic_Scraper_Ultimate，保留独立运行能力。

集成模式:
  from xiaoyan.scraper import scrape_cnki, scrape_wos

独立运行:
  python -m xiaoyan.scraper cnki --query "SU='人工智能'" --pages 3
"""

from .cnki_ultimate import scrape_cnki  # noqa: F401
from .wos_ultimate import scrape_wos    # noqa: F401
