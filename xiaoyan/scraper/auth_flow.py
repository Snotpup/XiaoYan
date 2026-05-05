"""
xiaoyan.scraper.auth_flow — 学术平台认证向导
================================================
为 CNKI / WoS 提供一次性浏览器认证入口。

原则:
  - 不保存、不自动填写学校账号密码
  - 用户自行选择 IP 认证或校外机构认证
  - 认证完成后复用 scraper 的持久化浏览器目录
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright

from xiaoyan.config import DATA_DIR

from .browser_core import launch_persistent_browser, dismiss_popups
from .config import (
    CNKI_SEARCH_URL,
    CNKI_STATE_DIR,
    WOS_INTL_SEARCH_URL,
    WOS_STATE_DIR,
)

CNKI_CARSI_URL = "https://fsso.cnki.net/"
WOS_CARSI_URL = "https://webofscience.com/UA"

AUTH_MODE_LABELS = {
    "ip": "校园网 / VPN / 机构 IP 认证",
    "carsi": "校外机构认证 / CARSI / 统一身份认证",
}

PLATFORM_LABELS = {
    "cnki": "知网 (CNKI)",
    "wos": "Web of Science (WoS)",
}


def auth_start_url(platform: str, mode: str) -> str:
    """返回指定平台和认证模式的起始 URL。"""
    if platform == "cnki":
        return CNKI_CARSI_URL if mode == "carsi" else CNKI_SEARCH_URL
    if platform == "wos":
        return WOS_CARSI_URL if mode == "carsi" else WOS_INTL_SEARCH_URL
    raise ValueError(f"未知平台: {platform}")


def auth_state_dir(platform: str) -> str:
    """返回平台对应的持久化浏览器状态目录。"""
    if platform == "cnki":
        return CNKI_STATE_DIR
    if platform == "wos":
        return WOS_STATE_DIR
    raise ValueError(f"未知平台: {platform}")


def write_auth_status(
    platform: str,
    mode: str,
    *,
    institution: str = "",
    verified: bool = False,
    note: str = "",
) -> Path:
    """记录最近一次认证结果，供 doctor/用户排查使用。"""
    data_dir = Path(DATA_DIR)
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / "auth_status.json"

    if path.exists():
        try:
            status = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            status = {}
    else:
        status = {}

    status[platform] = {
        "platform": platform,
        "platform_label": PLATFORM_LABELS.get(platform, platform),
        "mode": mode,
        "mode_label": AUTH_MODE_LABELS.get(mode, mode),
        "institution": institution,
        "verified": verified,
        "note": note,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }

    path.write_text(
        json.dumps(status, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


async def run_platform_auth(
    platform: str,
    mode: str,
    *,
    institution: str = "",
) -> bool:
    """
    打开持久化浏览器，引导用户完成平台认证。

    Returns:
        True 表示轻量验证通过；False 表示用户可能仍需重试。
    """
    if platform not in PLATFORM_LABELS:
        raise ValueError(f"未知平台: {platform}")
    if mode not in AUTH_MODE_LABELS:
        raise ValueError(f"未知认证方式: {mode}")

    start_url = auth_start_url(platform, mode)
    state_dir = auth_state_dir(platform)

    async with async_playwright() as p:
        context = await launch_persistent_browser(
            p,
            state_dir,
            show_browser=True,
        )
        page = await context.new_page()

        try:
            print(f"\n  打开认证入口: {start_url}")
            await page.goto(start_url, wait_until="domcontentloaded", timeout=60000)
            await dismiss_popups(page, platform)

            print()
            if mode == "carsi":
                _print_carsi_guidance(platform, institution)
            else:
                _print_ip_guidance(platform)

            input("  完成认证并看到平台页面后，按 Enter 让小研验证并保存会话...")

            if platform == "wos":
                print("  正在打开 WoS 国际站高级检索页确认会话可用...")
                await page.goto(
                    WOS_INTL_SEARCH_URL,
                    wait_until="domcontentloaded",
                    timeout=60000,
                )
                await dismiss_popups(page, platform)

            verified, note = await _verify_auth_page(page, platform)
            write_auth_status(
                platform,
                mode,
                institution=institution,
                verified=verified,
                note=note,
            )

            if verified:
                print(f"  ✓ {PLATFORM_LABELS[platform]} 认证状态看起来可用")
            else:
                print(f"  ⚠ 未能自动确认认证状态: {note}")
                print("    这不一定代表失败；如页面已经显示机构名或检索框，可先继续使用。")

            return verified

        finally:
            await context.close()


def _print_ip_guidance(platform: str):
    """打印 IP 认证操作提示。"""
    if platform == "cnki":
        print("  请确认浏览器已进入知网高级检索页。")
        print("  如果出现滑块/拼图验证码，请手动完成。")
    else:
        print("  请确认浏览器已进入 WoS 高级检索页。")
        print("  如果出现 Cloudflare 或人机验证，请手动完成。")


def _print_carsi_guidance(platform: str, institution: str):
    """打印校外机构认证操作提示。"""
    inst = institution or "你的学校/机构"
    if platform == "cnki":
        print("  请选择或搜索你的高校/机构，然后进入统一身份认证。")
        print(f"  当前记录的机构名: {inst}")
        print("  登录成功后，通常会跳转回 CNKI；能看到机构名或检索页面即可。")
    else:
        print("  请在 Institutional Sign In / 机构登录中选择联合认证。")
        print("  国内高校通常选择 CHINA CERNET Federation，再搜索学校/机构。")
        print(f"  当前记录的机构名: {inst}")
        print("  登录成功后，通常会跳转回 Web of Science；能看到机构名或高级检索页即可。")


async def _verify_auth_page(page, platform: str) -> tuple[bool, str]:
    """
    轻量验证页面是否具备继续自动检索的条件。

    验证目标刻意保守：只判断是否出现检索入口或机构访问信号，
    不发起真实检索，避免把认证向导变成爬取任务。
    """
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=15000)
    except Exception:
        pass

    try:
        await dismiss_popups(page, platform)
    except Exception:
        pass

    if platform == "cnki":
        selectors = [
            ".textarea-q2",
            "textarea.textarea-major",
            "textarea",
            "a:has-text('退出')",
            "a:has-text('机构')",
        ]
    else:
        selectors = [
            "textarea",
            "app-advanced-search",
        ]

    for selector in selectors:
        try:
            loc = page.locator(selector).first
            if await loc.count() > 0:
                return True, f"命中页面元素: {selector}"
        except Exception:
            continue

    try:
        title = await page.title()
        url = page.url
    except Exception:
        title, url = "", ""

    if platform == "cnki" and "cnki" in url.lower():
        return True, "页面位于 CNKI 域名"
    if platform == "wos" and ("webofscience" in url.lower() or "webofknowledge" in url.lower()):
        return False, "已到 WoS 域名但未发现高级检索输入框"

    return False, f"未发现可确认的检索入口 (title={title[:60]}, url={url[:120]})"
