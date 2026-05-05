"""
xiaoyan.setup — 环境检测与自动化引导工具
==========================================
提供依赖检测、自动安装、lark-cli 交互封装等功能，
服务于 `xiaoyan init` 一键式配置流程。

设计原则:
  - 每个函数只做一件事
  - 所有外部命令调用都有超时和错误处理
  - 彩色输出用 ANSI 转义，无额外依赖
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


# ============================================================
#  ANSI 颜色辅助 (无额外依赖)
# ============================================================

class _C:
    """轻量级 ANSI 颜色，自动检测终端支持"""
    _enabled = sys.stdout.isatty()

    @staticmethod
    def _wrap(code: str, text: str) -> str:
        if not _C._enabled:
            return text
        return f"\033[{code}m{text}\033[0m"

    @staticmethod
    def green(t: str) -> str:  return _C._wrap("32", t)
    @staticmethod
    def red(t: str) -> str:    return _C._wrap("31", t)
    @staticmethod
    def yellow(t: str) -> str: return _C._wrap("33", t)
    @staticmethod
    def cyan(t: str) -> str:   return _C._wrap("36", t)
    @staticmethod
    def bold(t: str) -> str:   return _C._wrap("1", t)
    @staticmethod
    def dim(t: str) -> str:    return _C._wrap("2", t)


def ok(msg: str):   print(f"  {_C.green('✅')} {msg}")
def fail(msg: str): print(f"  {_C.red('❌')} {msg}")
def warn(msg: str): print(f"  {_C.yellow('⚠')}  {msg}")
def info(msg: str): print(f"  {_C.cyan('ℹ')}  {msg}")
def step(title: str): print(f"\n{_C.bold(_C.cyan(title))}")


# ============================================================
#  依赖检测与安装
# ============================================================

def check_node() -> bool:
    """检测 Node.js / npm 是否可用"""
    npm = shutil.which("npm")
    if npm:
        try:
            ver = subprocess.check_output(
                ["node", "--version"], text=True, timeout=5
            ).strip()
            ok(f"Node.js: {ver}")
            return True
        except Exception:
            pass
    fail("Node.js / npm 未找到")
    print(f"     {_C.dim('请安装: brew install node')}")
    return False


def check_and_install_lark_cli() -> bool:
    """检测 lark-cli，未安装则自动安装"""
    if shutil.which("lark-cli"):
        ok("lark-cli: 已安装")
        return True

    info("lark-cli 未安装，正在自动安装...")
    try:
        subprocess.run(
            ["npm", "install", "-g", "@larksuite/cli"],
            check=True, timeout=120,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        if shutil.which("lark-cli"):
            ok("lark-cli: 安装成功")
            return True
    except subprocess.CalledProcessError as e:
        fail(f"lark-cli 安装失败: {e}")
    except subprocess.TimeoutExpired:
        fail("lark-cli 安装超时")

    print(f"     {_C.dim('请手动安装: npm install -g @larksuite/cli')}")
    return False


def check_and_install_playwright() -> bool:
    """检测 Playwright + Chromium，未安装则自动安装"""
    try:
        import playwright
        ok("Playwright: 已安装")
    except ImportError:
        fail("Playwright 未安装")
        print(f"     {_C.dim('请运行: pip install playwright')}")
        return False

    # 检测 chromium 浏览器
    try:
        result = subprocess.run(
            [sys.executable, "-c",
             "from playwright.sync_api import sync_playwright; "
             "p = sync_playwright().start(); "
             "b = p.chromium.launch(headless=True); b.close(); p.stop(); "
             "print('ok')"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0 and "ok" in result.stdout:
            ok("Chromium 浏览器: 就绪")
            return True
    except Exception:
        pass

    info("正在安装 Chromium 浏览器 (首次需要，约 100MB)...")
    try:
        subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            check=True, timeout=300,
        )
        ok("Chromium 浏览器: 安装成功")
        return True
    except Exception as e:
        fail(f"Chromium 安装失败: {e}")
        print(f"     {_C.dim('请手动运行: playwright install chromium')}")
        return False


def check_python_deps() -> bool:
    """检测 Python 依赖"""
    deps = {
        "dotenv": "python-dotenv",
        "openai": "openai",
        "apscheduler": "apscheduler",
        "pyperclip": "pyperclip",
        "yaml": "PyYAML",
    }
    all_ok = True
    for mod, pkg in deps.items():
        try:
            __import__(mod)
            ok(f"{pkg}: 已安装")
        except ImportError:
            fail(f"{pkg}: 缺失")
            print(f"     {_C.dim(f'pip install {pkg}')}")
            all_ok = False
    return all_ok


# ============================================================
#  lark-cli 配置与授权
# ============================================================

def lark_is_configured() -> bool:
    """检查 lark-cli 是否已配置 (有 App ID)"""
    try:
        result = subprocess.run(
            ["lark-cli", "config", "show"],
            capture_output=True, text=True, timeout=10,
        )
        # lark-cli 输出 JSON 中字段名为 appId (驼峰)
        return result.returncode == 0 and "appid" in result.stdout.lower()
    except Exception:
        return False


def lark_config_init() -> bool:
    """执行 lark-cli config init --new (会打开浏览器)"""
    print()
    info("即将打开浏览器创建飞书应用...")
    info("请在浏览器中完成应用创建，完成后此处会自动继续。")
    print()
    try:
        result = subprocess.run(
            ["lark-cli", "config", "init", "--new"],
            timeout=300,
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        fail("操作超时 (5 分钟)")
        return False
    except Exception as e:
        fail(f"创建飞书应用失败: {e}")
        return False


def get_lark_app_id() -> str:
    """从 lark-cli config show 中提取 App ID"""
    try:
        result = subprocess.run(
            ["lark-cli", "config", "show"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            config = json.loads(result.stdout)
            return config.get("appId", config.get("app_id", ""))
    except Exception:
        pass
    return ""


def lark_is_authed() -> bool:
    """检查 lark-cli 是否有用户登录 (非仅 bot)"""
    try:
        result = subprocess.run(
            ["lark-cli", "auth", "status"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return False
        out = result.stdout.lower()
        # "no user logged in" 表示只有 bot 身份，用户未登录
        if "no user logged in" in out:
            return False
        if "expired" in out:
            return False
        return True
    except Exception:
        return False


def lark_auth_login() -> bool:
    """执行 lark-cli auth login --recommend (会打开浏览器)"""
    print()
    info("即将打开浏览器进行飞书授权...")
    info("请在浏览器中点击「授权」，完成后此处会自动继续。")
    print()
    try:
        result = subprocess.run(
            ["lark-cli", "auth", "login", "--recommend"],
            timeout=300,
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        fail("授权超时 (5 分钟)")
        return False
    except Exception as e:
        fail(f"飞书授权失败: {e}")
        return False


def lark_list_chats() -> list[dict]:
    """
    通过 lark-cli 获取机器人可见的群聊列表。

    Returns:
        [{"chat_id": "oc_xxx", "name": "群名", "description": "..."}, ...]
    """
    try:
        result = subprocess.run(
            ["lark-cli", "im", "+chat-search",
             "--query", "",
             "--format", "json",
             "--as", "user",
             "--page-size", "20"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            return []

        data = json.loads(result.stdout)

        chats = []
        # lark-cli 返回格式可能有不同结构
        items = data if isinstance(data, list) else data.get("items", [])
        for item in items:
            chat_id = item.get("chat_id", "")
            name = item.get("name", "未命名群聊")
            desc = item.get("description", "")
            if chat_id:
                chats.append({
                    "chat_id": chat_id,
                    "name": name,
                    "description": desc,
                })
        return chats

    except Exception:
        return []


def lark_create_chat(name: str = "小研测试群") -> str:
    """
    用 bot 身份创建一个新群聊。

    Returns:
        成功返回 chat_id (oc_xxx)，失败返回空字符串。
    """
    try:
        result = subprocess.run(
            ["lark-cli", "im", "+chat-create",
             "--name", name,
             "--description", "小研科研助理的消息推送群",
             "--set-bot-manager",
             "--format", "json",
             "--as", "bot"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            return ""

        data = json.loads(result.stdout)
        # 实际返回格式: {"ok": true, "data": {"chat_id": "oc_xxx", "share_link": "..."}}
        chat_id = data.get("chat_id", "")
        share_link = ""
        if not chat_id:
            inner = data.get("data", {})
            if isinstance(inner, dict):
                chat_id = inner.get("chat_id", "")
                share_link = inner.get("share_link", "")
        # 存储 share_link 以供后续展示
        if share_link:
            lark_create_chat._last_share_link = share_link
        return chat_id

    except Exception:
        return ""

# 存储最后一次创建的群分享链接
lark_create_chat._last_share_link = ""


def choose_chat_id() -> str:
    """
    自动获取群聊列表并让用户选择。
    如果没有群聊，提供「自动创建」选项。
    如果获取失败也回退到手动输入。
    """
    info("正在获取你的飞书群聊列表...")
    chats = lark_list_chats()

    if chats:
        print()
        print(f"  {_C.bold('可用的群聊:')}")
        for i, chat in enumerate(chats, 1):
            name = chat["name"]
            cid = _C.dim(chat["chat_id"][:15] + "...")
            print(f"    [{_C.cyan(str(i))}] {name}  {cid}")

        print(f"    [{_C.cyan('0')}] 手动输入 Chat ID")
        print()

        while True:
            choice = input(f"  请选择 (1-{len(chats)}, 0=手动输入): ").strip()
            if choice == "0":
                break
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(chats):
                    selected = chats[idx]
                    ok(f"已选择: {selected['name']}")
                    return selected["chat_id"]
            except ValueError:
                pass
            print(f"  {_C.yellow('请输入有效的序号')}")
    else:
        warn("没有找到可用的群聊")
        print()
        print(f"  {_C.bold('你可以:')}")
        print(f"    [{_C.cyan('1')}] 自动创建一个「小研测试群」(推荐)")
        print(f"    [{_C.cyan('2')}] 手动输入 Chat ID")
        print(f"    [{_C.cyan('3')}] 跳过，稍后再配置")
        print()

        choice = input("  请选择 (1-3) [1]: ").strip() or "1"

        if choice == "1":
            info("正在创建「小研测试群」...")
            chat_id = lark_create_chat("小研测试群")
            if chat_id:
                ok(f"群聊已创建: {chat_id}")
                share_link = lark_create_chat._last_share_link
                if share_link:
                    info(f"加入群聊: {share_link}")
                else:
                    info("请在飞书 App 中找到「小研测试群」并加入")
                return chat_id
            else:
                fail("群聊创建失败")
                info("可能需要先完成用户授权: lark-cli auth login --recommend")
        elif choice == "3":
            warn("可稍后在 .env 中补充 XIAOYAN_LARK_CHAT_ID")
            return ""

    # 手动输入
    while True:
        chat_id = input("  飞书聊天 ID (oc_xxx，直接回车跳过): ").strip()
        if chat_id.startswith("oc_") and len(chat_id) > 5:
            return chat_id
        if chat_id == "":
            warn("可稍后在 .env 中补充 XIAOYAN_LARK_CHAT_ID")
            return ""
        print(f"  {_C.yellow('格式不对，应以 oc_ 开头，直接回车可跳过')}")


# ============================================================
#  LLM 配置
# ============================================================

# 国内常用 LLM 服务商预设
LLM_PRESETS = {
    "1": {
        "name": "豆包 (Doubao / 火山引擎)",
        "base_url": "https://ark.cn-beijing.volces.com/api/v3",
        "model": "doubao-seed-2-0-lite-260215",
        "key_hint": "xxx-xxx-xxx (console.volcengine.com → 模型推理 → API Key)",
    },
    "2": {
        "name": "DeepSeek",
        "base_url": "https://api.deepseek.com/v1",
        "model": "deepseek-chat",
        "key_hint": "sk-... (deepseek.com → API Keys)",
    },
    "3": {
        "name": "智谱 AI (GLM)",
        "base_url": "https://open.bigmodel.cn/api/paas/v4",
        "model": "glm-4-flash",
        "key_hint": "xxx.xxx (open.bigmodel.cn → API Keys)",
    },
    "4": {
        "name": "通义千问 (Qwen)",
        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "model": "qwen-turbo",
        "key_hint": "sk-... (dashscope.console.aliyun.com → API Key)",
    },
    "5": {
        "name": "OpenAI",
        "base_url": "https://api.openai.com/v1",
        "model": "gpt-4o-mini",
        "key_hint": "sk-... (platform.openai.com → API Keys)",
    },
    "6": {
        "name": "自定义 (兼容 OpenAI 格式)",
        "base_url": "",
        "model": "",
        "key_hint": "",
    },
}


def choose_llm_config() -> dict:
    """
    让用户选择 LLM 服务商并填入 API Key。

    Returns:
        {"api_key": ..., "base_url": ..., "model": ...}
    """
    print()
    print(f"  {_C.bold('选择 LLM 服务商:')}")
    for key, preset in LLM_PRESETS.items():
        print(f"    [{_C.cyan(key)}] {preset['name']}")
    print()

    while True:
        choice = input("  请选择 (1-6): ").strip()
        if choice in LLM_PRESETS:
            break
        print(f"  {_C.yellow('请输入 1-6')}")

    preset = LLM_PRESETS[choice]
    config = {}

    if choice == "6":
        # 自定义
        config["base_url"] = input("  API Base URL: ").strip()
        config["model"] = input("  模型名称: ").strip()
    else:
        config["base_url"] = preset["base_url"]
        default_model = preset["model"]
        model_input = input(
            f"  模型名称 [{_C.dim(default_model)}]: "
        ).strip()
        config["model"] = model_input or default_model

    # API Key
    key_hint = preset["key_hint"]
    if key_hint:
        print(f"  {_C.dim('格式: ' + key_hint)}")

    config["api_key"] = input("  API Key: ").strip()

    return config


def test_llm_connectivity(api_key: str, base_url: str, model: str) -> bool:
    """测试 LLM API 连通性"""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key, base_url=base_url, timeout=15)
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "请回复两个字: 连接成功"}],
            max_tokens=10,
        )
        content = resp.choices[0].message.content or ""
        ok(f"LLM API 连通性: 通过  ({model} → \"{content[:20]}\")")
        return True
    except Exception as e:
        err_msg = str(e)[:80]
        fail(f"LLM API 连通性: 失败 ({err_msg})")
        return False


# ============================================================
#  Cron 表达式辅助
# ============================================================

CRON_PRESETS = {
    "1": ("每 6 小时 (推荐)", "0 */6 * * *"),
    "2": ("每天早上 9 点",    "0 9 * * *"),
    "3": ("每 12 小时",       "0 */12 * * *"),
    "4": ("每天早晚各一次",    "0 9,21 * * *"),
    "5": ("自定义 cron",       ""),
}


def choose_cron() -> str:
    """让用户选择推送频率"""
    print()
    print(f"  {_C.bold('定时推送频率:')}")
    for key, (label, _) in CRON_PRESETS.items():
        print(f"    [{_C.cyan(key)}] {label}")
    print()

    while True:
        choice = input(f"  请选择 (1-5) [{_C.dim('1')}]: ").strip() or "1"
        if choice in CRON_PRESETS:
            break
        print(f"  {_C.yellow('请输入 1-5')}")

    _, cron = CRON_PRESETS[choice]
    if choice == "5":
        cron = input("  Cron 表达式: ").strip()
        if not cron:
            cron = "0 */6 * * *"
            warn(f"使用默认值: {cron}")

    return cron
