"""
browser_core.py — 浏览器引擎 + 反封锁工具集
=============================================
提供持久化浏览器启动、人类行为模拟、剪贴板注入、
弹窗清扫和人工验证接管窗口等核心能力。
所有爬虫模块共享此基础设施。

迁移自 Academic_Scraper_Ultimate，改为包内相对导入。
"""

import random
import pyperclip
from playwright.async_api import async_playwright, BrowserContext, Page

from .config import (
    CHROME_PATH, USER_AGENT, VIEWPORT, PASTE_MODIFIER,
    HUMAN_DELAY_MIN, HUMAN_DELAY_MAX,
    HUMAN_VERIFICATION_TIMEOUT,
)


# ============================================================
#  持久化浏览器启动
# ============================================================

async def launch_persistent_browser(
    playwright,
    state_dir: str,
    show_browser: bool = False,
) -> BrowserContext:
    """
    启动持久化 Chrome 浏览器上下文。
    - state_dir: 持久化目录，首次验证后 Cookie 永久复用
    - show_browser: True 显示界面 (用于首次人工验证码)
    """
    chrome_args = [
        "--disable-blink-features=AutomationControlled",
        "--no-first-run",
        "--no-default-browser-check",
    ]
    if not show_browser:
        chrome_args.append("--headless=new")

    try:
        context = await playwright.chromium.launch_persistent_context(
            user_data_dir=state_dir,
            headless=False,  # headless 由 --headless=new 参数控制
            executable_path=CHROME_PATH,
            ignore_default_args=["--enable-automation"],
            args=chrome_args,
            viewport=VIEWPORT,
            user_agent=USER_AGENT,
            accept_downloads=True,
        )
    except Exception as e:
        print(f"  ⚠ 带缓存启动失败 ({e})，尝试无自定义路径降级启动...")
        context = await playwright.chromium.launch_persistent_context(
            user_data_dir=state_dir,
            headless=False,
            ignore_default_args=["--enable-automation"],
            args=chrome_args,
            viewport=VIEWPORT,
            user_agent=USER_AGENT,
            accept_downloads=True,
        )

    return context


# ============================================================
#  人类行为模拟
# ============================================================

async def human_delay(page: Page, min_ms: int = None, max_ms: int = None):
    """随机延迟，模拟人类操作节奏"""
    lo = min_ms or HUMAN_DELAY_MIN
    hi = max_ms or HUMAN_DELAY_MAX
    await page.wait_for_timeout(random.randint(lo, hi))


async def human_scroll(page: Page, distance: int = None):
    """模拟人类滚动页面"""
    dist = distance or random.randint(200, 600)
    await page.mouse.wheel(0, dist)
    await human_delay(page, 300, 800)


# ============================================================
#  剪贴板无痕注入
# ============================================================

async def clipboard_paste(page: Page, text: str, target_selector: str = None):
    """
    通过系统剪贴板粘贴文本，完美模拟真人 Ctrl/Cmd+V。
    绕过前端输入字符频率监测。
    """
    pyperclip.copy(text)

    if target_selector:
        locator = page.locator(target_selector).first
        await locator.click()
        await human_delay(page, 300, 600)

    # 先清空已有内容
    await page.keyboard.press(f"{PASTE_MODIFIER}+a")
    await page.keyboard.press("Backspace")
    await human_delay(page, 200, 400)

    # 粘贴
    await page.keyboard.press(f"{PASTE_MODIFIER}+v")
    await human_delay(page, 500, 1000)


# ============================================================
#  多层弹窗清扫 (通用 + 平台专属)
# ============================================================

async def dismiss_popups(page: Page, platform: str = "general"):
    """
    自动关闭各种常见覆盖弹窗：
    - Cookie 同意框
    - 反馈提醒框
    - 右上角关闭按钮
    """
    popup_selectors = []

    if platform in ("wos", "general"):
        popup_selectors.extend([
            ("button:has-text('Accept all')", "Cookie 同意框"),
            ("button:has-text('Accept All Cookies')", "Cookie 全接受"),
            ("button#onetrust-accept-btn-handler", "OneTrust Cookie"),
            ("button:has-text('Remind me later')", "稍后提醒"),
            (".cdk-overlay-container button.close-button", "CDK 覆盖关闭"),
            ("button:has-text('Got it')", "知晓按钮"),
            ("button:has-text('Close')", "关闭按钮"),
        ])

    if platform in ("cnki", "general"):
        popup_selectors.extend([
            (".dialog-close", "知网弹窗关闭"),
            ("button:has-text('关闭')", "中文关闭按钮"),
        ])

    for selector, name in popup_selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count() > 0 and await locator.is_visible():
                await locator.click(timeout=2000)
                print(f"  ✓ 已关闭弹窗: {name}")
                await human_delay(page, 300, 600)
        except Exception:
            pass  # 弹窗未出现或已关闭，静默跳过


# ============================================================
#  人工验证接管窗口
# ============================================================

async def wait_for_human_verification(
    page: Page,
    target_selector: str,
    show_browser: bool,
    platform: str = "未知平台",
    timeout: int = None,
) -> bool:
    """
    等待目标元素出现。如果超时且显示浏览器，则留给人工手动操作。
    返回 True 表示目标元素已出现；False 表示彻底超时。
    """
    wait_ms = timeout or HUMAN_VERIFICATION_TIMEOUT

    print(f"\n{'=' * 60}")
    print(f"  【安全接管期】{platform}")
    print(f"  如遇验证码 / Cloudflare 挑战，请在 {wait_ms // 1000} 秒内手动完成。")
    print(f"  目标: 等待 '{target_selector}' 出现")
    print(f"{'=' * 60}\n")

    try:
        await page.wait_for_selector(target_selector, timeout=wait_ms)
        print(f"  ✓ 目标元素已就绪，继续自动执行。")
        return True
    except Exception:
        if show_browser:
            print(f"\n  ✗ 等待超时。请检查浏览器窗口是否需要手动操作。")
        else:
            print(f"\n  ✗ 等待超时！")
            print(f"  >>> 补救指南: 请将 show_browser 设为 True 运行一次以手动完成验证，")
            print(f"  >>> 之后即可恢复无痕模式。<<<")
        return False
