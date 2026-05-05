"""
cnki_ultimate.py — CNKI 终极爬虫
=================================
融合 AI4R 的反封锁能力 + 智研TRACK 的数据管理体系。

核心特性:
  ✅ 持久化会话 (一次滑块，永久免验)
  ✅ 剪贴板注入 (绕过字符频率监测)
  ✅ 单 Context 多 Page (避免线程池爆炸)
  ✅ SQLite 哈希去重 + 运行日志
  ✅ 智能翻页 (自动检测末页)
  ✅ 双模式: 显示浏览器 / 完全后台静默
"""

import asyncio
import re
from playwright.async_api import async_playwright

from .config import (
    CNKI_SEARCH_URL, CNKI_STATE_DIR, CNKI_SOURCE_MAP,
    PAGE_TURN_DELAY_MIN, PAGE_TURN_DELAY_MAX,
    CNKI_DEFAULT_MAX_PAGES, SCRAPE_GLOBAL_TIMEOUT,
    CNKI_DETAIL_CONCURRENCY, CNKI_DETAIL_DELAY_MS,
)
from .browser_core import (
    launch_persistent_browser, human_delay, clipboard_paste,
    dismiss_popups, wait_for_human_verification,
)
from .db_manager import (
    get_papers_db, init_cnki_table, init_run_log_table,
    cnki_paper_exists, insert_cnki_paper, log_run,
    generate_hash, update_task_total,
    get_tasks_db,
)


# ============================================================
#  列表页数据提取
# ============================================================

async def extract_list_page(page, task_id: int, query: str) -> list:
    """
    从当前知网结果列表页提取论文基础信息。
    返回 paper_data 列表 (不含摘要，摘要在详情页单独获取)。
    """
    papers = []

    try:
        await page.wait_for_selector("table tbody tr td.name", timeout=30000)
        await human_delay(page, 1500, 3000)
    except Exception:
        print("  ⚠ 列表加载超时")
        await page.screenshot(path="cnki_list_timeout.png")
        return papers

    rows = await page.locator("table tbody tr:has(td.name)").all()
    print(f"  当页共 {len(rows)} 条记录")

    for row in rows:
        try:
            title_el = row.locator("td.name a").first
            title = (await title_el.inner_text()).strip()
            href = await title_el.get_attribute("href")

            # 作者
            try:
                authors = (await row.locator("td.author").inner_text()).strip()
            except Exception:
                authors = ""

            # 来源期刊
            try:
                journal = (await row.locator("td.source").inner_text()).strip()
            except Exception:
                journal = ""

            # 发表日期
            try:
                date = (await row.locator("td.date").inner_text()).strip()
            except Exception:
                date = ""

            # 被引
            cited = 0
            try:
                cited_text = (await row.locator("td.quote").inner_text()).strip()
                if cited_text.isdigit():
                    cited = int(cited_text)
            except Exception:
                pass

            # 下载量
            downloads = 0
            try:
                dl_text = (await row.locator("td.download").inner_text()).strip()
                if dl_text.isdigit():
                    downloads = int(dl_text)
            except Exception:
                pass

            # 构造详情页 URL
            full_url = f"https://kns.cnki.net{href}" if href and href.startswith("/") else href

            data_hash = generate_hash(title, authors, journal, date)

            papers.append({
                "task_id": task_id,
                "title": title,
                "authors": authors,
                "journal": journal,
                "publish_date": date,
                "cited_count": cited,
                "download_count": downloads,
                "detail_url": full_url,
                "abstract": None,
                "keywords": None,
                "data_hash": data_hash,
            })
        except Exception as e:
            print(f"  ⚠ 行数据提取失败: {e}")
            continue

    return papers


# ============================================================
#  详情页摘要 + 关键词获取 (单 Context, 异步并发 Page)
#  迁移自智研TRACK v2.1 的并发详情页思路:
#  - v2.1 用 ThreadPoolExecutor(5) + 独立浏览器实例 → 资源浪费
#  - 升级为 asyncio.Semaphore + 单 Context 多 Page → 轻量高效
# ============================================================

async def fetch_detail(context, paper: dict) -> dict:
    """
    用同一个 BrowserContext 异步打开新标签页获取摘要和关键词，
    获取完毕立即关闭标签页，避免资源堆积。
    """
    detail_page = await context.new_page()
    try:
        await detail_page.goto(paper["detail_url"], wait_until="domcontentloaded", timeout=30000)
        await human_delay(detail_page, 1500, 2500)

        # 摘要
        abs_locator = detail_page.locator("#ChDivSummary, .abstract-text").first
        if await abs_locator.count() > 0:
            paper["abstract"] = (await abs_locator.inner_text()).strip()
        else:
            # 备用选择器
            abs_alt = detail_page.locator("#abstract_text").first
            if await abs_alt.count() > 0:
                paper["abstract"] = (await abs_alt.get_attribute("value") or "").strip()
            else:
                paper["abstract"] = ""

        # 关键词
        kw_elements = detail_page.locator("p.keywords a")
        kw_count = await kw_elements.count()
        if kw_count > 0:
            kws = []
            for i in range(kw_count):
                kw = (await kw_elements.nth(i).inner_text()).strip().rstrip(";").strip()
                if kw:
                    kws.append(kw)
            paper["keywords"] = "; ".join(kws)

    except Exception as e:
        paper["abstract"] = paper.get("abstract") or f"获取失败: {e}"
    finally:
        await detail_page.close()
        await asyncio.sleep(CNKI_DETAIL_DELAY_MS / 1000)  # 可配置冷却延迟

    return paper


async def fetch_details_batch(context, papers: list, concurrency: int = None) -> list:
    """
    并发获取多篇论文的详情页 (摘要 + 关键词)。
    使用 Semaphore 控制同时打开的标签页数量，避免资源爆炸。

    迁移自智研TRACK v2.1 的 ThreadPoolExecutor 并发思路，
    改为 asyncio.gather + Semaphore 实现，复用单一 BrowserContext。

    Args:
        context: Playwright BrowserContext (所有标签页共享 Cookie/Session)
        papers: 需要获取详情的论文列表
        concurrency: 并发数 (默认取 config.CNKI_DETAIL_CONCURRENCY)

    Returns:
        更新了 abstract 和 keywords 的论文列表
    """
    max_concurrent = concurrency or CNKI_DETAIL_CONCURRENCY
    sem = asyncio.Semaphore(max_concurrent)

    async def _fetch_one(paper):
        async with sem:
            return await fetch_detail(context, paper)

    # 只对有详情 URL 的论文并发获取
    to_fetch = [p for p in papers if p.get("detail_url")]
    no_url = [p for p in papers if not p.get("detail_url")]

    if to_fetch:
        print(f"  ⚡ 并发获取 {len(to_fetch)} 篇详情 (并发度={max_concurrent})")
        fetched = await asyncio.gather(*[_fetch_one(p) for p in to_fetch])
        return list(fetched) + no_url
    return papers


# ============================================================
#  核心爬取流程
# ============================================================

async def scrape_cnki(
    task_id: int,
    query: str,
    source_filters: list = None,
    max_pages: int = None,
    show_browser: bool = False,
):
    """
    CNKI 终极爬取主函数。

    参数:
      task_id: 任务 ID (用于数据库隔离)
      query: 专业检索式
      source_filters: 来源过滤列表, 如 ["北大核心", "CSSCI"]
      max_pages: 最大翻页数, None = 全量
      show_browser: 是否显示浏览器界面
    """
    source_filters = source_filters or []
    # 安全默认值: 未指定 max_pages 时使用配置中的默认值
    if max_pages is None:
        max_pages = CNKI_DEFAULT_MAX_PAGES
    elif max_pages <= 0:
        max_pages = None  # 显式传入 0 或 -1 表示不限
    table_name = f"cnki_task_{task_id}"

    # 初始化数据库
    conn = get_papers_db()
    init_cnki_table(conn, table_name)
    init_run_log_table(conn)

    total_found = 0
    total_new = 0
    status = "failed"

    async with async_playwright() as p:
        print(f"\n{'=' * 60}")
        print(f"  CNKI 终极爬虫启动")
        print(f"  任务 ID: {task_id} | 检索式: {query}")
        print(f"  来源过滤: {source_filters or '无'}")
        print(f"  界面模式: {'显示' if show_browser else '静默'}")
        print(f"{'=' * 60}\n")

        context = await launch_persistent_browser(p, CNKI_STATE_DIR, show_browser)
        page = await context.new_page()

        try:
            # --- 导航 ---
            print("[1/5] 导航至知网高级检索...")
            await page.goto(CNKI_SEARCH_URL, wait_until="domcontentloaded")
            await human_delay(page, 2000, 3000)

            # --- 关闭 Chrome "Restore pages?" 弹窗 (持久化会话残留) ---
            try:
                restore_btn = page.locator("button:has-text('Restore'), button:has-text('Don\'t restore')")
                if await restore_btn.count() > 0:
                    await restore_btn.last.click(timeout=2000)
                    print("  ✓ 已关闭 Chrome Restore 弹窗")
                    await human_delay(page, 500, 1000)
            except Exception:
                pass

            # --- 切换专业检索 ---
            print("[2/5] 切换至专业检索...")
            try:
                await page.locator("li[name='majorSearch']").first.click(force=True, timeout=5000)
                await human_delay(page, 1000, 2000)
            except Exception:
                print("  (专业检索可能已激活)")

            # --- 检测验证码重定向 + 等待输入框 ---
            target_sel = ".textarea-q2, textarea.textarea-major, textarea"

            # 检查是否被重定向到了验证码页面
            current_url = page.url
            if "/verify/" in current_url:
                print("  ⚠ 知网触发了安全验证 (验证码页面)")
                if show_browser:
                    print("  请在浏览器中完成拼图/滑块验证...")
                    print("  完成后程序将自动继续。")
                    # 等待 URL 离开 /verify/ 页面 (最多 180 秒)
                    try:
                        await page.wait_for_url(
                            lambda url: "/verify/" not in url,
                            timeout=180_000,
                        )
                        print("  ✓ 验证通过！正在重新导航到搜索页...")
                        await human_delay(page, 1000, 2000)
                        # 验证通过后重新导航到搜索页
                        await page.goto(CNKI_SEARCH_URL, wait_until="domcontentloaded")
                        await human_delay(page, 2000, 3000)
                    except Exception:
                        print("  ✗ 验证超时 (180秒)")
                        status = "captcha_blocked"
                        await context.close()
                        return
                else:
                    print("  ✗ 无头模式下无法完成验证码！")
                    print("  >>> 请使用 --show-browser 模式运行一次以手动完成验证 <<<")
                    status = "captcha_blocked"
                    await context.close()
                    return

            # 尝试等待 textarea 出现 (短超时，因为此时应该已在搜索页)
            textarea_ready = False
            try:
                await page.wait_for_selector(target_sel, timeout=15_000)
                textarea_ready = True
            except Exception:
                pass

            if not textarea_ready:
                # 可能在导航过程中又被拦截，或者页面结构不同
                # 再尝试切换专业检索后等一次
                try:
                    await page.locator("li[name='majorSearch']").first.click(force=True, timeout=5000)
                    await human_delay(page, 1000, 2000)
                    await page.wait_for_selector(target_sel, timeout=15_000)
                    textarea_ready = True
                except Exception:
                    pass

            if not textarea_ready:
                if not await wait_for_human_verification(
                    page, target_sel, show_browser, platform="知网"
                ):
                    status = "captcha_blocked"
                    await context.close()
                    return

            await dismiss_popups(page, "cnki")
            # 关闭可能的知网弹窗信息框 (如"请输入正确检索表达式"残留)
            try:
                confirm_btn = page.locator("a.layui-layer-btn0, button:has-text('确定')")
                if await confirm_btn.count() > 0 and await confirm_btn.first.is_visible():
                    await confirm_btn.first.click(timeout=2000)
                    print("  ✓ 已关闭知网信息弹窗")
            except Exception:
                pass
            await human_delay(page, 1000, 2000)

            # --- 来源勾选 ---
            if source_filters:
                print(f"[3/5] 勾选来源类别: {source_filters}")
                # 展开"学术期刊"
                try:
                    await page.locator("li:has-text('学术期刊')").click(timeout=3000)
                    await human_delay(page, 500, 1000)
                except Exception:
                    pass

                # 取消"全部期刊"
                try:
                    all_j = page.locator("label:has-text('全部期刊')").locator("input[type='checkbox']")
                    if await all_j.count() > 0 and await all_j.is_checked():
                        await all_j.uncheck(force=True)
                        await human_delay(page, 400, 800)
                except Exception:
                    pass

                for source in source_filters:
                    cb = page.locator(f"label:has-text('{source}')").locator("input[type='checkbox']")
                    if await cb.count() > 0 and not await cb.is_checked():
                        await cb.check(force=True)
                        await human_delay(page, 400, 800)
            else:
                print("[3/5] 无来源过滤，跳过")

            # --- 剪贴板注入检索式 ---
            print(f"[4/5] 剪贴板注入检索式: {query[:60]}...")

            # 定位 textarea (兼容多种知网版式)
            textarea_sel = None
            for sel in [".textarea-q2", "textarea.textarea-major", "textarea"]:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    textarea_sel = sel
                    break

            if not textarea_sel:
                print("  ✗ 未找到任何可用的检索输入框！")
                await page.screenshot(path="cnki_no_textarea.png")
                status = "no_textarea"
                await context.close()
                return

            textarea = page.locator(textarea_sel).first

            # 策略 1: 先尝试点击 textarea 聚焦后用剪贴板粘贴
            paste_success = False
            try:
                await textarea.click(force=True)
                await human_delay(page, 300, 600)
                await clipboard_paste(page, query)
                await human_delay(page, 500, 800)

                # 验证: 检查文本是否真的写入了
                content = await textarea.input_value()
                if not content:
                    content = await textarea.inner_text()
                if content and len(content.strip()) > 0:
                    paste_success = True
                    print(f"  ✓ 剪贴板注入成功 ({len(content)} 字符)")
            except Exception as e:
                print(f"  ⚠ 剪贴板粘贴遇到问题: {e}")

            # 策略 2: 如果剪贴板失败，降级为 fill() 直接填入
            if not paste_success:
                print("  ↓ 降级为 fill() 直接填入...")
                try:
                    await textarea.click(force=True)
                    await textarea.fill(query)
                    await human_delay(page, 500, 800)
                    content = await textarea.input_value()
                    if not content:
                        content = await textarea.inner_text()
                    if content and len(content.strip()) > 0:
                        print(f"  ✓ fill() 填入成功 ({len(content)} 字符)")
                    else:
                        # 策略 3: 最后尝试逐字符 type()
                        print("  ↓ 再降级为 type() 逐字输入...")
                        await textarea.click(force=True)
                        await textarea.type(query, delay=50)
                        await human_delay(page, 500, 800)
                        print("  ✓ type() 输入完毕")
                except Exception as e2:
                    print(f"  ✗ 所有输入方式均失败: {e2}")
                    await page.screenshot(path="cnki_input_failed.png")
                    status = "input_failed"
                    await context.close()
                    return

            await human_delay(page, 800, 1500)

            # --- 触发检索 ---
            print("[5/5] 触发检索...")
            search_btn = page.locator("input[value='检索']").first
            if await search_btn.count() == 0:
                search_btn = page.locator(".btn-search").first
            await search_btn.click()

            # 检测检索后的错误弹窗 ("请输入正确检索表达式")
            await human_delay(page, 1000, 2000)
            try:
                err_dialog = page.locator(".layui-layer-content:has-text('检索'), .layui-layer-content:has-text('输入')")
                if await err_dialog.count() > 0 and await err_dialog.first.is_visible():
                    err_text = await err_dialog.first.inner_text()
                    print(f"  ✗ 知网报告错误: {err_text}")
                    # 关闭弹窗
                    ok_btn = page.locator("a.layui-layer-btn0")
                    if await ok_btn.count() > 0:
                        await ok_btn.first.click()
                    status = "query_error"
                    await context.close()
                    return
            except Exception:
                pass  # 无错误弹窗，继续正常流程

            # --- 等待结果加载 ---
            try:
                await page.wait_for_selector("table tbody tr td.name", timeout=30000)
                await human_delay(page, 2000, 3000)
            except Exception:
                print("  ✗ 结果加载超时，可能无检索结果")
                await page.screenshot(path="cnki_no_results.png")
                status = "no_results"
                await context.close()
                return

            # --- 读取总数 ---
            try:
                total_el = page.locator("span.pagerTitleCell em")
                if await total_el.count() > 0:
                    total_text = (await total_el.inner_text()).strip().replace(",", "")
                    if total_text.isdigit():
                        total_found = int(total_text)
                        print(f"\n  ★ 知网报告总结果数: {total_found}")
                        if max_pages:
                            max_papers = max_pages * 20
                            print(f"  ★ 安全限制: 最多翻 {max_pages} 页 ({max_papers} 条)")
                        if total_found > 1000:
                            print(f"  ⚠ 结果数量较大，建议缩小检索范围或添加时间限定")
                        # 回写任务数据库
                        try:
                            tasks_conn = get_tasks_db()
                            update_task_total(tasks_conn, task_id, total_found)
                            tasks_conn.close()
                        except Exception:
                            pass
            except Exception:
                pass

            # --- 尝试按发表日期排序 (优先获取最新文献) ---
            try:
                date_sort = page.locator("a.sort-default:has-text('发表日期'), a:has-text('日期')")
                if await date_sort.count() > 0:
                    await date_sort.first.click()
                    await human_delay(page, 1500, 2500)
                    # 确保是降序 (最新在前)
                    # 知网的排序按钮点一次升序，再点一次降序
                    # 检查是否有降序标识
                    try:
                        await page.wait_for_selector("table tbody tr td.name", timeout=15000)
                    except Exception:
                        pass
                    print("  ✓ 已切换为按发表日期排序")
            except Exception:
                pass  # 排序切换失败不影响主流程

            # --- 批量翻页提取 ---
            current_page = 1
            while True:
                print(f"\n--- 第 {current_page} 页 ---")
                papers = await extract_list_page(page, task_id, query)

                # 筛选出需要获取详情的新论文
                new_papers = [
                    p for p in papers
                    if not cnki_paper_exists(conn, p["data_hash"], table_name)
                ]

                if new_papers:
                    print(f"  新论文 {len(new_papers)} 篇，并发获取详情中...")
                    # 升级: 用 asyncio.gather 并发获取详情, 替代串行逐篇
                    enriched = await fetch_details_batch(context, new_papers)
                    for paper in enriched:
                        if insert_cnki_paper(conn, paper, table_name):
                            total_new += 1
                            print(f"    ++ {paper['title'][:40]}...")
                else:
                    print(f"  本页全部已存在，更新时间戳")

                # --- 智能翻页检测 ---
                if max_pages and current_page >= max_pages:
                    print(f"  已达设定最大页数 ({max_pages})，停止")
                    break

                next_btn = page.locator("a#PageNext")
                if await next_btn.count() > 0 and await next_btn.is_enabled():
                    # 检查是否真的还有下一页
                    page_count_on_page = len(papers)
                    if page_count_on_page < 20 and current_page > 1:
                        print("  当前页不满 20 条，判断为最后一页")
                        break

                    print("  翻至下一页...")
                    await next_btn.click()
                    await human_delay(page, PAGE_TURN_DELAY_MIN, PAGE_TURN_DELAY_MAX)

                    # 等待新页面加载
                    try:
                        await page.wait_for_selector("table tbody tr td.name", timeout=30000)
                        await human_delay(page, 1500, 2500)
                    except Exception:
                        print("  下一页加载超时，结束翻页")
                        break

                    current_page += 1
                else:
                    print("  无下一页按钮，翻页结束")
                    break

            status = "success"

        except Exception as e:
            print(f"\n  ✗ 爬取异常: {e}")
            import traceback
            traceback.print_exc()
            status = "failed"
        finally:
            try:
                log_run(conn, task_id, "cnki", query, total_found, total_new, status)
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass

    conn.close()

    print(f"\n{'=' * 60}")
    print(f"  CNKI 爬取完毕")
    print(f"  状态: {status}")
    print(f"  总结果数: {total_found}")
    print(f"  本次新增: {total_new}")
    print(f"{'=' * 60}\n")


# ============================================================
#  独立运行入口
# ============================================================

if __name__ == "__main__":
    # 示例: 直接运行
    asyncio.run(scrape_cnki(
        task_id=1,
        query="SU=('人工智能' + '医疗') * '深度学习'",
        source_filters=["北大核心", "CSSCI"],
        max_pages=3,
        show_browser=True,
    ))
