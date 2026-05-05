"""
xiaoyan.cli — 命令行入口 + 一键式配置引导
=============================================
提供三个核心命令:
  - xiaoyan init:    一键式环境检测 + 依赖安装 + 配置引导
  - xiaoyan start:   启动小研常驻服务
  - xiaoyan doctor:  诊断环境就绪状态 (带彩色输出和修复建议)

设计原则:
  - 一条命令搞定所有配置，用户无需手动编辑文件
  - 自动检测并安装依赖，最少化人工干预
  - 友好的交互式引导，用选择代替输入
"""

import argparse
import asyncio
import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path


def setup_logging(verbose: bool = False):
    """配置日志"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


# ============================================================
#  Banner
# ============================================================

_BANNER = """
╔══════════════════════════════════════════════════════════╗
║          🧑‍🔬  小研 (XiaoYan) — 首次配置引导            ║
║                                                          ║
║    一条命令完成: 环境检测 → 依赖安装 → 飞书配置 →        ║
║                  LLM 配置 → 首次爬虫认证                 ║
╚══════════════════════════════════════════════════════════╝
"""


# ============================================================
#  交互式配置引导 (增强版)
# ============================================================

def run_init():
    """一键式配置引导: 从零到可运行"""
    from xiaoyan.setup import (
        _C, ok, fail, warn, info, step,
        check_node, check_and_install_lark_cli,
        check_and_install_playwright, check_python_deps,
        lark_is_configured, lark_config_init,
        lark_is_authed, lark_auth_login,
        choose_chat_id, choose_llm_config,
        test_llm_connectivity, choose_cron,
    )

    project_root = Path(__file__).parent.parent
    env_path = project_root / ".env"

    print(_BANNER)

    # ── 检查已有配置 ──
    if env_path.exists():
        print(f"  {_C.yellow('⚠')}  已存在 .env 配置文件。")
        choice = input("     重新配置？[y/N]: ").strip().lower()
        if choice != "y":
            print("  保留现有配置。运行 xiaoyan doctor 可检查环境状态。")
            return

    # ════════════════════════════════════════════════════════
    #  Phase 1: 环境检测与依赖安装
    # ════════════════════════════════════════════════════════

    step("🔍 Phase 1/5: 环境检测")
    blockers = []

    # Node.js
    has_node = check_node()
    if not has_node:
        blockers.append("Node.js")

    # lark-cli (自动安装)
    if has_node:
        has_lark = check_and_install_lark_cli()
        if not has_lark:
            blockers.append("lark-cli")
    else:
        fail("lark-cli: 跳过 (需要先安装 Node.js)")
        blockers.append("lark-cli")

    # Python 依赖
    check_python_deps()

    # Playwright + Chromium (自动安装)
    check_and_install_playwright()

    if blockers:
        print()
        fail(f"以下依赖缺失，无法继续: {', '.join(blockers)}")
        print(f"\n  {_C.dim('解决后重新运行 xiaoyan init')}")
        sys.exit(1)

    # ════════════════════════════════════════════════════════
    #  Phase 2: 飞书应用配置
    # ════════════════════════════════════════════════════════

    step("🔑 Phase 2/5: 飞书应用配置")

    # 2a: 检查 / 创建飞书应用
    if lark_is_configured():
        ok("飞书应用: 已配置")
    else:
        info("需要创建飞书应用 (首次使用)")
        print()
        print(f"  {_C.bold('操作说明:')}")
        print(f"    1. 浏览器将打开飞书开放平台")
        print(f"    2. 按提示创建一个新应用")
        print(f"    3. 完成后此窗口会自动继续")
        print()

        proceed = input("  准备好了？按 Enter 开始... ").strip()
        if not lark_config_init():
            fail("飞书应用创建未完成")
            warn("你可以稍后手动运行: lark-cli config init --new")
        else:
            ok("飞书应用: 创建成功")

    # 2b: 登录授权
    if lark_is_authed():
        ok("飞书授权: 已登录")
    else:
        info("需要飞书用户授权 (首次使用)")
        print()
        print(f"  {_C.bold('操作说明:')}")
        print(f"    浏览器将打开授权页面，点击「授权」即可。")
        print()

        proceed = input("  准备好了？按 Enter 开始... ").strip()
        if not lark_auth_login():
            fail("飞书授权未完成")
            warn("你可以稍后手动运行: lark-cli auth login --recommend")
        else:
            ok("飞书授权: 成功")

    # ════════════════════════════════════════════════════════
    #  Phase 3: 选择目标群聊
    # ════════════════════════════════════════════════════════

    step("💬 Phase 3/5: 选择目标群聊")
    print(f"  {_C.dim('小研需要知道向哪个群发送消息。')}")

    chat_id = choose_chat_id()

    # ════════════════════════════════════════════════════════
    #  Phase 4: LLM 配置
    # ════════════════════════════════════════════════════════

    step("🤖 Phase 4/5: LLM 配置")
    print(f"  {_C.dim('小研使用 LLM 进行意图识别和文献评述。')}")
    print(f"  {_C.dim('支持任何兼容 OpenAI 格式的 API。')}")

    llm = choose_llm_config()

    # API 连通性测试
    if llm["api_key"]:
        print()
        info("正在验证 LLM API 连通性...")
        success = test_llm_connectivity(
            llm["api_key"], llm["base_url"], llm["model"]
        )
        if not success:
            retry = input("  是否仍然保存此配置？[Y/n]: ").strip().lower()
            if retry == "n":
                print("  请核实 API 信息后重新运行 xiaoyan init")
                return

    # ════════════════════════════════════════════════════════
    #  Phase 5: 调度 + 收尾
    # ════════════════════════════════════════════════════════

    step("⏰ Phase 5/5: 定时策略")
    print(f"  {_C.dim('选择小研自动查找新文献的频率。')}")

    cron = choose_cron()

    # ── 写入 .env ──
    print()
    info("正在保存配置...")
    lines = [
        "# ──────────────────────────────────────────────────────────",
        "#  小研 (XiaoYan) 配置文件",
        f"#  由 xiaoyan init 自动生成于 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "# ──────────────────────────────────────────────────────────",
        "",
        "# --- LLM (OpenAI 兼容格式) ---",
        f"XIAOYAN_LLM_API_KEY={llm['api_key']}",
        f"XIAOYAN_LLM_BASE_URL={llm['base_url']}",
        f"XIAOYAN_LLM_MODEL={llm['model']}",
        "",
        "# --- 飞书 ---",
        f"XIAOYAN_LARK_CHAT_ID={chat_id}",
        "",
        "# --- 定时调度 ---",
        f"XIAOYAN_DEFAULT_CRON={cron}",
        "",
    ]
    env_path.write_text("\n".join(lines))
    ok(f"配置已保存: {env_path}")

    # ── 初始化数据库 ──
    info("正在初始化数据库...")
    from xiaoyan.core.db import init_db
    init_db()
    ok("数据库初始化完成")

    # ── 首次爬虫认证 (推荐) ──
    print()
    print(f"  {'─' * 56}")
    print(f"  {_C.bold('推荐: 首次学术平台认证')}")
    print()
    print(f"  {_C.dim('知网和 WoS 首次访问时会弹出验证码 (拼图/滑块)。')}")
    print(f"  {_C.dim('在浏览器中手动完成一次后，Cookie 保存在本地，')}")
    print(f"  {_C.dim('后续小研自动爬取时无需再次验证。')}")
    print()
    print(f"  {_C.dim('耗时: 约 1~2 分钟 | 之后可用 xiaoyan auth 重新认证')}")
    print()

    do_auth = input("  现在进行首次认证？(推荐) [Y/n]: ").strip().lower()
    if do_auth != "n":
        _run_first_scraper_auth()
    else:
        info("跳过。稍后可运行:")
        print(f"    {_C.cyan('xiaoyan auth')}")

    # ── 完成 ──
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║                    🎉 配置完成！                        ║")
    print("╠══════════════════════════════════════════════════════════╣")
    print("║                                                          ║")
    print("║  启动小研:                                               ║")
    print("║    xiaoyan start                                         ║")
    print("║                                                          ║")
    print("║  检查环境:                                               ║")
    print("║    xiaoyan doctor                                        ║")
    print("║                                                          ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()


def _run_first_scraper_auth():
    """引导用户在浏览器中完成首次爬虫验证（init 流程调用）"""
    run_auth(called_from_init=True)


def run_auth(called_from_init: bool = False):
    """
    独立的浏览器认证命令 (xiaoyan auth)。
    打开浏览器引导用户完成知网/WoS 的访问授权。

    设计要点:
      - 小白友好: 每一步都有清晰说明
      - 验证后会话保存在本地，后续无头模式自动复用
      - 支持校园网/IP 认证，也支持校外机构认证 (CARSI/统一身份认证)
    """
    from xiaoyan.setup import info, ok, warn, fail, step, _C

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║            🌐 学术平台浏览器认证                        ║")
    print("╠══════════════════════════════════════════════════════════╣")
    print("║                                                          ║")
    print("║  小研需要一次性获得学术平台访问授权。                      ║")
    print("║  授权后，浏览器会话会保存在本地，后续自动复用。            ║")
    print("║                                                          ║")
    print("║  ⚡ 可选择校园网/IP 认证，也可选择校外机构认证。           ║")
    print("║  📍 如果会话过期，重新运行 xiaoyan auth 即可刷新。         ║")
    print("║                                                          ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    # --- 认证方式说明 ---
    print(f"  {_C.bold('认证方式说明:')}")
    print(f"    [{_C.cyan('1')}] 校园网 / VPN / 机构 IP 认证")
    print(f"        {_C.dim('适合已经在学校网络、校园 VPN 或机构授权 IP 范围内。')}")
    print(f"    [{_C.cyan('2')}] 校外机构认证 / CARSI / 统一身份认证")
    print(f"        {_C.dim('适合不在 IP 范围内，通过学校账号登录数据库资源。')}")
    print()

    # --- 平台选择 ---
    platforms = []
    print(f"  {_C.bold('选择需要认证的平台:')}")
    print(f"    [{_C.cyan('1')}] 知网 (CNKI)")
    print(f"    [{_C.cyan('2')}] Web of Science (WoS)")
    print(f"    [{_C.cyan('3')}] 两个都做 (推荐)")
    print()

    choice = input("  请选择 (1-3) [3]: ").strip() or "3"
    if choice in ("1", "3"):
        platforms.append("cnki")
    if choice in ("2", "3"):
        platforms.append("wos")

    from xiaoyan.scraper.auth_flow import run_platform_auth

    for platform in platforms:
        print()
        print(f"  {'─' * 56}")

        mode = _choose_platform_auth_mode(_C)
        institution = ""
        if mode == "carsi":
            institution = input(
                "  请输入学校/机构名称（可留空，认证页面内再搜索）: "
            ).strip()

        if platform == "cnki":
            step("🔑 知网 (CNKI) 认证")
            print()
            print(f"  {_C.bold('即将发生:')}")
            if mode == "carsi":
                print(f"    1. 浏览器会打开知网校外访问入口")
                print(f"    2. 你选择自己的学校/机构并完成统一身份认证")
                print(f"    3. 登录成功后回到 CNKI 页面，按 Enter 保存会话")
            else:
                print(f"    1. 浏览器会打开知网高级检索页")
                print(f"    2. 如出现拼图/滑块验证码，请手动完成")
                print(f"    3. 页面可用后，按 Enter 保存会话")
            print()
            input(f"  准备好了？按 Enter 开始...")
            print()

            try:
                verified = asyncio.run(
                    run_platform_auth(
                        "cnki",
                        mode,
                        institution=institution,
                    )
                )
                if verified:
                    ok("知网认证已保存 ✨ 后续将复用该浏览器会话")
                else:
                    warn("知网认证已保存，但未能自动确认状态；如不可用可稍后重试")
            except Exception as e:
                fail(f"知网认证出错: {e}")

        else:
            step("🔑 Web of Science (WoS) 认证")
            print()
            print(f"  {_C.bold('即将发生:')}")
            if mode == "carsi":
                print(f"    1. 浏览器会打开 WoS 机构访问入口")
                print(f"    2. 选择联合认证/机构登录，并搜索自己的学校/机构")
                print(f"    3. 完成统一身份认证后回到 WoS 页面，按 Enter 保存会话")
            else:
                print(f"    1. 浏览器会打开 WoS 高级检索页")
                print(f"    2. 如出现 Cloudflare 或人机验证，请手动完成")
                print(f"    3. 页面可用后，按 Enter 保存会话")
            print()
            input(f"  准备好了？按 Enter 开始...")
            print()

            try:
                verified = asyncio.run(
                    run_platform_auth(
                        "wos",
                        mode,
                        institution=institution,
                    )
                )
                if verified:
                    ok("WoS 认证已保存 ✨ 后续将复用该浏览器会话")
                else:
                    warn("WoS 认证已保存，但未能自动确认状态；如不可用可稍后重试")
            except Exception as e:
                fail(f"WoS 认证出错: {e}")

    # 认证结果汇总
    print()
    print(f"  {'─' * 56}")
    if not called_from_init:
        print(f"  {_C.bold('认证完成！')}")
        print(f"  浏览器会话已保存到本地，后续爬取将自动复用。")
        print(f"  如需重新认证: {_C.cyan('xiaoyan auth')}")
        print()


def _choose_platform_auth_mode(_C) -> str:
    """交互式选择当前平台的认证方式。"""
    print(f"  {_C.bold('请选择本平台认证方式:')}")
    print(f"    [{_C.cyan('1')}] 校园网 / VPN / 机构 IP 认证")
    print(f"    [{_C.cyan('2')}] 校外机构认证 / CARSI / 统一身份认证")
    print()

    while True:
        choice = input("  请选择 (1-2) [1]: ").strip() or "1"
        if choice == "1":
            return "ip"
        if choice == "2":
            return "carsi"
        print(f"  {_C.yellow('请输入 1 或 2')}")


# ============================================================
#  启动服务
# ============================================================

def run_start(verbose: bool = False):
    """启动小研常驻服务"""
    setup_logging(verbose)
    logger = logging.getLogger("xiaoyan")

    logger.info("🚀 正在启动小研...")

    from xiaoyan.core.engine import XiaoYanEngine

    engine = XiaoYanEngine()
    asyncio.run(engine.start())


# ============================================================
#  环境诊断 (xiaoyan doctor)
# ============================================================

def run_doctor():
    """全面诊断环境就绪状态，输出检查结果和修复建议"""
    from xiaoyan.setup import (
        _C, ok, fail, warn, info, step,
        check_node, check_python_deps,
        lark_is_configured, lark_is_authed,
    )

    print()
    print(f"{'═' * 60}")
    print(f"  🩺 小研 (XiaoYan) — 环境诊断")
    print(f"{'═' * 60}")

    issues = []

    # ── 1. 系统工具 ──
    step("系统工具")
    check_node()

    lark_cli = shutil.which("lark-cli")
    if lark_cli:
        ok(f"lark-cli: {lark_cli}")
    else:
        fail("lark-cli: 未安装")
        issues.append("npm install -g @larksuite/cli")

    # ── 2. Python 依赖 ──
    step("Python 依赖")
    check_python_deps()

    # Playwright 单独检查
    try:
        import playwright
        ok("playwright: 已安装")
    except ImportError:
        fail("playwright: 缺失")
        issues.append("pip install playwright && playwright install chromium")

    # ── 3. 飞书状态 ──
    step("飞书状态")
    if lark_cli:
        if lark_is_configured():
            ok("飞书应用: 已配置")
        else:
            fail("飞书应用: 未配置")
            issues.append("lark-cli config init --new")

        if lark_is_authed():
            ok("飞书授权: 已登录")
        else:
            warn("飞书授权: 未登录或已过期")
            issues.append("lark-cli auth login --recommend")
    else:
        warn("飞书状态: 跳过 (lark-cli 未安装)")

    # ── 4. 配置文件 ──
    step("配置检查")
    project_root = Path(__file__).parent.parent
    env_path = project_root / ".env"
    if env_path.exists():
        ok(f".env 文件: {env_path}")

        from xiaoyan.config import (
            validate_config, LLM_BASE_URL, LLM_MODEL,
            LARK_CHAT_ID, DB_PATH,
        )
        missing = validate_config()
        if missing:
            for m in missing:
                fail(f"配置缺失: {m}")
            issues.append("xiaoyan init")
        else:
            ok(f"LLM: {LLM_MODEL} @ {_C.dim(LLM_BASE_URL)}")
            if LARK_CHAT_ID:
                masked = LARK_CHAT_ID[:10] + "..." if len(LARK_CHAT_ID) > 10 else LARK_CHAT_ID
                ok(f"Chat ID: {masked}")
            else:
                warn("Chat ID: 未配置 (无法发送消息)")
    else:
        fail(".env 文件: 不存在")
        issues.append("xiaoyan init")

    # ── 5. 数据库 ──
    step("数据存储")
    from xiaoyan.config import DB_PATH, DATA_DIR
    ok(f"数据目录: {DATA_DIR}")
    if DB_PATH.exists():
        # 统计检索式数量
        try:
            from xiaoyan.core.db import list_search_queries
            queries = list_search_queries(active_only=False)
            active = sum(1 for q in queries if q.get("is_active"))
            ok(f"数据库: {DB_PATH.name} ({len(queries)} 条检索式, {active} 条活跃)")
        except Exception:
            ok(f"数据库: {DB_PATH}")
    else:
        info("数据库: 尚未创建 (首次启动时自动创建)")

    # ── 6. 学术平台认证状态 ──
    step("学术平台认证")
    auth_status_path = DATA_DIR / "auth_status.json"
    if auth_status_path.exists():
        try:
            import json
            status = json.loads(auth_status_path.read_text(encoding="utf-8"))
            if status:
                for platform in ("cnki", "wos"):
                    item = status.get(platform)
                    if not item:
                        continue
                    verified_label = "已验证" if item.get("verified") else "未自动确认"
                    institution = item.get("institution") or "未记录机构"
                    mode_label = item.get("mode_label", item.get("mode", ""))
                    updated = item.get("updated_at", "")
                    if item.get("verified"):
                        ok(
                            f"{item.get('platform_label', platform)}: "
                            f"{mode_label} ({verified_label}, {institution}, {updated})"
                        )
                    else:
                        warn(
                            f"{item.get('platform_label', platform)}: "
                            f"{mode_label} ({verified_label}, {institution}, {updated})"
                        )
            else:
                info("尚无认证记录，可运行 xiaoyan auth")
        except Exception as e:
            warn(f"认证状态读取失败: {e}")
    else:
        info("尚无认证记录，可运行 xiaoyan auth")

    # ── 汇总 ──
    print()
    print(f"{'─' * 60}")
    if not issues:
        print(f"  {_C.green(_C.bold('✅ 一切就绪！'))} 运行 {_C.cyan('xiaoyan start')} 启动服务。")
    else:
        print(f"  {_C.yellow(_C.bold(f'⚠ 发现 {len(issues)} 个问题，建议运行:'))}")
        print()
        for cmd in issues:
            print(f"    {_C.cyan('$')} {cmd}")
        print()
        print(f"  或运行 {_C.cyan('xiaoyan init')} 一键修复。")
    print()


# ============================================================
#  主入口
# ============================================================

def main():
    # 'status' 是 'doctor' 的静默别名
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        sys.argv[1] = "doctor"

    parser = argparse.ArgumentParser(
        description="小研 (XiaoYan) — 基于飞书的私人科研助理",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
命令:
  init     一键式配置引导 (环境检测 + 依赖安装 + 配置)
  auth     学术平台浏览器认证 (首次/会话过期时使用)
  start    启动常驻服务
  doctor   诊断环境就绪状态

示例:
  xiaoyan init          # 首次使用，一条命令搞定
  xiaoyan auth          # 完成/刷新学术平台验证
  xiaoyan start         # 启动服务
  xiaoyan doctor        # 环境诊断
        """,
    )

    sub = parser.add_subparsers(dest="command")

    sub.add_parser("init", help="一键式配置引导")
    sub.add_parser("auth", help="学术平台浏览器认证 (首次/会话过期时使用)")

    start_p = sub.add_parser("start", help="启动常驻服务")
    start_p.add_argument("-v", "--verbose", action="store_true", help="详细日志")

    sub.add_parser("doctor", help="诊断环境就绪状态")

    args = parser.parse_args()

    if args.command == "init":
        run_init()
    elif args.command == "auth":
        run_auth()
    elif args.command == "start":
        run_start(verbose=args.verbose)
    elif args.command == "doctor":
        run_doctor()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
