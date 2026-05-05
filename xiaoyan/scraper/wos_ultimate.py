"""
wos_ultimate.py — Web of Science 终极爬虫
==========================================
融合 AI4R 的 Zero-Jump 快速提取 + 智研TRACK 的 BibTeX 深度导出。

双模式架构:
  🚀 快速模式 (quick):  列表页原位展开摘要，零跳转，高容错
  📦 深度模式 (deep):   官方 BibTeX 导出，全字段落库

核心特性:
  ✅ 持久化会话 (Session 复用)
  ✅ 多层弹窗自动清扫
  ✅ 超长人工验证接管窗口 (Cloudflare / hCaptcha)
  ✅ BibTeX 解析入库 (含 UPSERT 去重)
  ✅ 冗余选择器体系 (适配 WoS 多版本界面)
"""

import asyncio
import os
import re
from datetime import datetime
from playwright.async_api import async_playwright

from .config import (
    WOS_SEARCH_URL, WOS_INTL_SEARCH_URL, WOS_STATE_DIR,
    WOS_MAX_EXPORT_PER_FILE, WOS_DOWNLOAD_DIR,
    PAGE_TURN_DELAY_MIN, PAGE_TURN_DELAY_MAX,
)
from .browser_core import (
    launch_persistent_browser, human_delay, human_scroll,
    dismiss_popups, wait_for_human_verification,
)
from .db_manager import (
    get_papers_db, init_wos_table, init_wos_quick_table, init_run_log_table,
    insert_wos_bibtex_paper, insert_wos_quick_paper, log_run,
    generate_hash, update_task_total,
    get_tasks_db,
)


# ============================================================
#  快速模式: Zero-Jump 列表页原位提取
# ============================================================

async def extract_quick_mode(page, context, task_id: int, max_records: int = 20):
    """
    不做任何页面跳转，直接在搜索结果列表中：
    1. 定位每条记录的摘要展开按钮并点击
    2. 原位读取摘要文本
    3. 提取标题和期刊名
    """
    conn = get_papers_db()
    table_name = f"wos_quick_{task_id}"
    init_wos_quick_table(conn, table_name)

    results = []
    seen_titles = set()

    # 等待记录容器 (冗余选择器: 优先 app-record, 降级 app-summary-record)
    # 2026-04 DOM 探测: WoS 已重构, app-summary-record → app-record
    record_sel = "app-record"
    record_sel_fallback = "app-summary-record, .summary-record"
    try:
        try:
            await page.wait_for_selector(record_sel, timeout=45000)
        except Exception:
            print("  ⚠ 主选择器 app-record 未命中, 尝试降级选择器...")
            await page.wait_for_selector(record_sel_fallback, timeout=15000)
            record_sel = record_sel_fallback
        await human_delay(page, 2000, 4000)
    except Exception:
        print("  ✗ 结果列表加载超时 (主+降级选择器均失败)")
        await page.screenshot(path="wos_quick_timeout.png")
        conn.close()
        return results

    blocks = await page.locator(record_sel).all()
    actual_count = min(max_records, len(blocks))
    print(f"  列表页挂载了 {len(blocks)} 条记录，提取前 {actual_count} 条")

    extracted = 0
    for i in range(len(blocks)):
        if extracted >= max_records:
            break

        block = blocks[i]

        # --- 标题 (冗余选择器: data-ta 优先, class 降级) ---
        title_loc = block.locator(
            "a[data-ta='summary-record-title-link'], "
            "a.title-link, a.title, h3 a, h2 a, .title a"
        ).first
        title = (await title_loc.inner_text()).strip() if await title_loc.count() > 0 else ""
        clean_title = title.replace("\n", " ").strip()
        if not clean_title or clean_title in seen_titles:
            continue
        seen_titles.add(clean_title)

        # --- 期刊 (冗余选择器: jcr-link-menu 优先, 多层降级) ---
        # 2026-04 DOM 探测: 期刊用 a[data-ta='jcr-link-menu'] class=summary-source-title-link
        source_loc = block.locator(
            "a[data-ta='jcr-link-menu'], "
            "a.summary-source-title-link, "
            "a[data-ta='summary-record-source-title-link'], "
            "app-jcr-overlay span, [data-ta='jcr-link'], "
            ".source-title, .journal-title, .sourceTitle"
        ).first
        journal = (await source_loc.inner_text()).strip() if await source_loc.count() > 0 else ""
        # 清理 Material Icon 残留文本 (WoS Angular 组件尾部)
        for noise in ["arrow_drop_down", "open_in_new"]:
            journal = journal.replace(noise, "").strip()

        # --- 展开摘要 ---
        print(f"  [{extracted + 1}] {clean_title[:50]}...")
        expand_btn = block.locator("button").filter(has_text="Abstract").first
        if await expand_btn.count() == 0:
            expand_btn = block.locator("button").filter(has_text="Show Abstract").first
        if await expand_btn.count() > 0:
            try:
                await expand_btn.scroll_into_view_if_needed()
                await expand_btn.click(timeout=3000)
                await human_delay(page, 600, 1200)
            except Exception:
                pass

        # 展开 "Show more"
        more_btn = block.locator("button:has-text('Show more'), button:has-text('Read more')").first
        if await more_btn.count() > 0:
            try:
                await more_btn.scroll_into_view_if_needed()
                await more_btn.click(timeout=1000)
                await human_delay(page, 400, 800)
            except Exception:
                pass

        # --- 提取摘要文本 ---
        abstract = ""
        abs_loc = block.locator(
            ".abstract-text, [id^='summary-abstract'], .abstract, [data-ta='abstract-record']"
        ).first
        if await abs_loc.count() > 0:
            abstract = (await abs_loc.inner_text()).strip()
        else:
            abstract = ""

        # 清理残留文本
        for noise in ["Show more expand_more", "Show more", "Show less expand_less", "Show less"]:
            abstract = abstract.replace(noise, "")
        abstract = abstract.replace("\n", " ").strip()

        data_hash = generate_hash(clean_title, "", journal, "")
        paper = {
            "task_id": task_id,
            "title": clean_title,
            "journal": journal.replace("\n", " ").strip(),
            "abstract": abstract or "原文无摘要",
            "data_hash": data_hash,
        }

        if insert_wos_quick_paper(conn, paper, table_name):
            results.append(paper)
            print(f"      ✓ 摘要 {len(abstract)} 字 | 期刊: {journal[:30]}")
        else:
            print(f"      - 已存在，跳过")

        extracted += 1

    conn.close()
    return results


# ============================================================
#  深度模式: BibTeX 官方导出
#  迁移借鉴: 智研TRACK WoS v2 的多批次导出恢复 + 任务目录隔离
# ============================================================

async def _open_bibtex_export_menu(page):
    """
    打开 Export → BibTeX 菜单流程 (独立函数以便多批次复用)。
    迁移自智研TRACK WoS v2 L459-471: 每次批量导出完成后
    WoS 的导出弹窗自动关闭, 需要重新走 Export → BibTeX 流程。
    """
    # 点击主 Export 按钮
    export_btn = page.locator("app-export-menu button:has-text('Export')").first
    await export_btn.wait_for(state="visible", timeout=15000)
    await export_btn.click()
    await human_delay(page, 1000, 2000)

    # 选择 BibTeX
    bib_btn = page.locator("button[id='exportToBibtexButton']:has-text('BibTeX')").first
    if await bib_btn.count() == 0:
        bib_btn = page.locator("button:has-text('BibTeX')").first
    await bib_btn.click()
    await human_delay(page, 1000, 2000)

    # 等待导出对话框
    await page.wait_for_selector("app-export-out-details", state="visible", timeout=15000)


async def export_bibtex_deep(page, context, task_id: int, total_papers: int):
    """
    通过 WoS 官方 Export -> BibTeX 流程，
    分批下载 .bib 文件并解析入库。

    迁移自智研TRACK WoS v2: 多批次导出恢复 + 任务目录隔离 + 错误截图归档。
    """
    conn = get_papers_db()
    table_name = f"wos_deep_{task_id}"
    init_wos_table(conn, table_name)

    # 任务目录隔离 (迁移自智研TRACK WoS v2 L360-363)
    task_dl_dir = os.path.join(WOS_DOWNLOAD_DIR, f"task_{task_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    os.makedirs(task_dl_dir, exist_ok=True)
    print(f"  📂 文件下载目录: {task_dl_dir}")

    files_exported = 0
    total_parsed = 0
    start_record = 1
    is_first_batch = True

    while start_record <= total_papers:
        end_record = min(start_record + WOS_MAX_EXPORT_PER_FILE - 1, total_papers)
        print(f"\n  [Export] 导出记录 {start_record} ~ {end_record} (共 {total_papers})")

        try:
            # 第一批: Export 菜单已在上层流程中可用
            # 后续批次: 需要重新打开 Export → BibTeX 流程
            # (迁移自智研TRACK WoS v2 L596-607)
            if is_first_batch:
                await _open_bibtex_export_menu(page)
                is_first_batch = False
            else:
                print("  🔄 重新打开导出流程...")
                await _open_bibtex_export_menu(page)

            # 选择 "Records from:" 单选
            try:
                label = page.locator("label[for='radio3-input']")
                await label.scroll_into_view_if_needed()
                await label.click(timeout=5000)
                await human_delay(page, 500, 1000)
            except Exception:
                # 降级: 直接 force check
                radio = page.locator("input#radio3-input")
                await radio.check(force=True, timeout=5000)
                await human_delay(page, 500, 1000)

            # 填写范围
            await page.wait_for_selector("input[name='markFrom']:not([disabled])", state="visible")
            await page.locator("input[name='markFrom']").fill(str(start_record))
            await page.locator("input[name='markTo']").fill(str(end_record))
            await human_delay(page, 500, 1000)

            # 选择 Record Content: Full Record and Cited References
            dropdown = page.locator("wos-select button[aria-haspopup='listbox']").first
            await dropdown.click()
            await human_delay(page, 800, 1500)

            try:
                await page.wait_for_selector("text='Full Record and Cited References'", state="attached", timeout=3000)
            except Exception:
                pass
            
            full_record_opt = None
            for sel in [
                "div[role='menuitem'][title='Full Record and Cited References']",
                ".cdk-overlay-pane span:has-text('Full Record and Cited References')",
                "text='Full Record and Cited References'"
            ]:
                loc = page.locator(sel).last
                if await loc.count() > 0:
                    full_record_opt = loc
                    break
            
            if full_record_opt is None:
                full_record_opt = page.locator("text='Full Record and Cited References'").last

            await full_record_opt.click()
            await human_delay(page, 800, 1500)

            # 点击 Export 下载
            export_confirm = page.locator("app-export-out-details button#exportButton")
            async with page.expect_download(timeout=120000) as dl_info:
                await export_confirm.click()

            download = await dl_info.value
            ts = datetime.now().strftime('%H%M%S')
            filename = f"wos_task{task_id}_{start_record}-{end_record}_{ts}.bib"
            save_path = os.path.join(task_dl_dir, filename)
            await download.save_as(save_path)
            files_exported += 1
            print(f"  ✓ 已下载: {filename}")

            # 解析并入库
            parsed = await parse_and_store_bib(save_path, conn, table_name, task_id)
            total_parsed += parsed
            print(f"  ✓ 已解析入库: {parsed} 条")

            await human_delay(page, 2000, 3000)

        except Exception as e:
            ts = datetime.now().strftime('%H%M%S')
            print(f"  ✗ 导出批次 {start_record}-{end_record} 失败: {e}")
            # 错误截图存到任务目录 (迁移自智研TRACK WoS v2 L504, L564)
            await page.screenshot(
                path=os.path.join(task_dl_dir, f"error_batch{start_record}_{ts}.png")
            )
            # 尝试关闭可能残留的弹窗
            try:
                close_btn = page.locator("button:has-text('Close'), button:has-text('Cancel')").first
                if await close_btn.count() > 0:
                    await close_btn.click(timeout=2000)
            except Exception:
                pass
            # 标记需要重新打开导出菜单 (因为弹窗状态可能已混乱)
            is_first_batch = True

        start_record += WOS_MAX_EXPORT_PER_FILE

    conn.close()
    return files_exported, total_parsed


async def parse_and_store_bib(filepath: str, conn, table_name: str, task_id: int) -> int:
    """解析 BibTeX 文件并存入数据库"""
    try:
        import bibtexparser
    except ImportError:
        print("  ⚠ bibtexparser 未安装，跳过 BibTeX 解析")
        print("  >>> 安装命令: pip install bibtexparser <<<")
        return 0

    count = 0
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            bib_db = bibtexparser.load(f)

        for entry in bib_db.entries:
            if insert_wos_bibtex_paper(conn, entry, table_name, task_id):
                count += 1
    except Exception as e:
        print(f"  ✗ BibTeX 解析错误: {e}")

    return count


# ============================================================
#  核心爬取流程
# ============================================================

async def scrape_wos(
    task_id: int,
    query: str,
    mode: str = "quick",
    max_records: int = 20,
    show_browser: bool = True,
    use_cn_site: bool | None = None,
    site: str = "com",
):
    """
    WoS 终极爬取主函数。

    参数:
      task_id: 任务 ID
      query: WoS 高级检索式, 如 "TS=(AI AND healthcare)"
      mode: "quick" (Zero-Jump) 或 "deep" (BibTeX 导出)
      max_records: 快速模式下最大提取数
      show_browser: 是否显示浏览器
      use_cn_site: 兼容旧调用；True 强制中国站 (.cn)
      site: "com" 或 "cn"，默认国际站 (.com)
    """
    conn = get_papers_db()
    init_run_log_table(conn)

    total_found = 0
    total_new = 0
    status = "failed"

    if use_cn_site is True:
        site = "cn"
    site = (site or "com").lower()
    if site not in ("com", "cn"):
        raise ValueError(f"未知 WoS 站点: {site}")

    search_url = WOS_SEARCH_URL if site == "cn" else WOS_INTL_SEARCH_URL
    site_label = "国际站 (.com)" if site == "com" else "中国站 (.cn)"

    async with async_playwright() as p:
        print(f"\n{'=' * 60}")
        print(f"  WoS 终极爬虫启动")
        print(f"  任务 ID: {task_id} | 模式: {mode.upper()}")
        print(f"  检索式: {query}")
        print(f"  站点: {site_label}")
        print(f"{'=' * 60}\n")

        context = await launch_persistent_browser(p, WOS_STATE_DIR, show_browser)
        page = await context.new_page()

        try:
            # --- 导航 ---
            print("[1/4] 导航至 WoS 高级检索...")
            await page.goto(search_url, wait_until="domcontentloaded")

            # --- 人工验证接管 (页面加载阶段) ---
            if not await wait_for_human_verification(
                page, "textarea", show_browser, platform="Web of Science"
            ):
                status = "captcha_blocked"
                await context.close()
                return status # conn 在 finally 后关闭

            await human_delay(page, 1500, 2500)

            # --- 弹窗清扫 ---
            print("[2/4] 清扫覆盖弹窗...")
            await dismiss_popups(page, "wos")

            # --- 输入检索式 ---
            print(f"[3/4] 输入检索式: {query[:60]}...")
            textarea = page.locator("textarea").first
            await textarea.scroll_into_view_if_needed()
            await textarea.click(force=True)
            await textarea.fill("")
            await textarea.fill(query)
            await human_delay(page, 1000, 2000)

            # --- 触发检索 ---
            print("[4/4] 触发检索...")
            # 冗余选择器: 精确 → 宽泛，逐一探测
            search_btn_selectors = [
                "button.search.mat-flat-button",             # WoS Angular Material 精确 class
                "button[data-ta='search-button']",            # data-ta 属性
                "section.search button.mat-flat-button",      # 搜索区域内的 Material 按钮
                "button.search-button",                       # 通用 class
            ]
            search_btn = None
            for sel in search_btn_selectors:
                btn = page.locator(sel).first
                if await btn.count() > 0:
                    search_btn = btn
                    print(f"  ✓ 搜索按钮命中: {sel}")
                    break
            if search_btn is None:
                # 最终降级: 文本匹配 (取最后一个, 避免匹配 Search History 等)
                search_btn = page.locator("button:has-text('Search')").last
            if await search_btn.count() > 0:
                await search_btn.click()
            else:
                await page.keyboard.press("Enter")

            # 等待结果加载 — 冗余选择器策略
            # 2026-04 DOM 探测: app-record 是当前有效的记录选择器
            # app-page-controls 也是可靠的结果页标志 (含总数信息)
            result_selectors = [
                "app-record",              # 当前有效 (2026-04 verified)
                "app-records-list",        # 列表容器
                "app-page-controls",       # 页面控件 (含总数)
                "app-summary-record",      # 历史选择器 (降级)
            ]
            results_loaded = False
            for sel in result_selectors:
                try:
                    await page.wait_for_selector(sel, timeout=15000)
                    results_loaded = True
                    print(f"  ✓ 结果页就绪 (选择器: {sel})")
                    break
                except Exception:
                    continue

            if not results_loaded:
                # 先检查是否是"无结果"页面 (而非验证码)
                no_results_indicators = [
                    "app-no-records",           # WoS 无结果组件
                    ".no-data",                  # 无数据提示
                    "text='No records found'",   # 英文提示文本
                    "text='Your search did not'", # 搜索无结果
                ]
                is_no_results = False
                for indicator in no_results_indicators:
                    try:
                        el = page.locator(indicator)
                        if await el.count() > 0:
                            is_no_results = True
                            break
                    except Exception:
                        pass

                # 再检查页面文本中是否包含无结果提示
                if not is_no_results:
                    try:
                        body_text = await page.inner_text("body", timeout=3000)
                        if any(kw in body_text for kw in [
                            "No records found",
                            "did not find any results",
                            "0 result",
                            "no result",
                        ]):
                            is_no_results = True
                    except Exception:
                        pass

                if is_no_results:
                    print("\n  ℹ WoS 搜索无结果 (非验证码问题)")
                    print("  建议: 调整检索关键词或时间范围后重试")
                    status = "no_results"
                    return status

                # 确认不是无结果后，才判断为可能遇到验证码
                print("\n  ⚠ 结果未加载 (所有选择器均超时)，可能遇到验证码...")
                await page.screenshot(path="wos_possible_captcha.png")

                if show_browser:
                    print("  请在浏览器中手动完成验证码，完成后程序将自动继续...")
                    try:
                        await page.wait_for_selector("app-record", timeout=120000)
                        results_loaded = True
                        print("  ✓ 验证通过，结果已加载")
                    except Exception:
                        pass
                else:
                    print("  >>> 提示: 请使用 --show-browser 模式运行以手动完成验证码 <<<")

            if not results_loaded:
                print("  ✗ 结果加载超时")
                await page.screenshot(path="wos_results_timeout.png")
                status = "captcha_blocked"
                return status # conn 和 context 在 finally/后续关闭

            await human_delay(page, 2000, 5000)  # WoS Angular SPA 需要额外时间渲染 page-controls

            # --- 读取总数 (冗余策略: app-page-controls 优先, 降级 mat-checkbox) ---
            # 2026-04 DOM 探测: app-page-controls 内容如 "0/16,421\nAdd To Marked List..."
            try:
                total_found = 0

                # 策略 1: app-page-controls 文本 (当前有效)
                for attempt in range(2):  # 至多两次尝试
                    pc_el = page.locator("app-page-controls").first
                    if await pc_el.count() > 0:
                        pc_text = (await pc_el.inner_text()).strip()
                        match = re.search(r"/\s*([\d,]+)", pc_text)
                        if match:
                            total_found = int(match.group(1).replace(",", ""))
                            break
                    if attempt == 0:
                        await human_delay(page, 2000, 3000)  # 等 Angular 渲染

                # 策略 2: 降级 mat-checkbox (历史选择器)
                if total_found == 0:
                    count_sel = "mat-checkbox[data-ta='select-page-checkbox'] label span.mat-checkbox-label"
                    count_el = page.locator(count_sel)
                    if await count_el.count() > 0:
                        count_text = (await count_el.inner_text()).strip()
                        match = re.search(r"/\s*([\d,]+)", count_text)
                        if match:
                            total_found = int(match.group(1).replace(",", ""))

                # 策略 3: 从 URL 中提取 (最后手段)
                if total_found == 0:
                    url_text = page.url
                    # WoS URL 不含总数, 但 body 文本中 "of X" 可能存在
                    try:
                        body_text = await page.inner_text("app-page-controls, .top-controls", timeout=3000)
                        match = re.search(r"of\s+([\d,]+)", body_text)
                        if match:
                            total_found = int(match.group(1).replace(",", ""))
                    except Exception:
                        pass

                if total_found > 0:
                    print(f"\n  ★ WoS 报告总结果数: {total_found}")
                    try:
                        tasks_conn = get_tasks_db()
                        update_task_total(tasks_conn, task_id, total_found)
                        tasks_conn.close()
                    except Exception:
                        pass
                else:
                    print("\n  ⚠ 未能读取结果总数 (不影响数据提取)")
            except Exception as e:
                print(f"  ⚠ 读取总数异常: {e}")

            # --- 执行模式分支 ---
            if mode == "quick":
                print("\n  🚀 快速模式 (Zero-Jump)")
                results = await extract_quick_mode(page, context, task_id, max_records)
                total_new = len(results)
                status = "success"

            elif mode == "deep":
                print("\n  📦 深度模式 (BibTeX 导出)")
                if total_found == 0:
                    print("  无结果可导出")
                    status = "success_no_results"
                else:
                    files, parsed = await export_bibtex_deep(page, context, task_id, total_found)
                    total_new = parsed
                    status = "success"
                    print(f"\n  导出文件: {files} 个, 解析入库: {parsed} 条")

                    # 自动匹配 JCR 期刊分区 (迁移自智研TRACK 分区查询.py)
                    if parsed > 0:
                        try:
                            from .journal_rank import load_jcr_data, enrich_wos_papers
                            jcr_data = load_jcr_data()
                            if jcr_data:
                                table_name = f"wos_deep_{task_id}"
                                matched, _ = enrich_wos_papers(table_name, jcr_data)
                                print(f"  📊 期刊分区匹配: {matched}/{parsed} 篇")
                        except Exception as e:
                            print(f"  ⚠ 期刊分区匹配跳过: {e}")
            else:
                print(f"  ✗ 未知模式: {mode}")
                status = "invalid_mode"

        except Exception as e:
            print(f"\n  ✗ 爬取异常: {e}")
            import traceback
            traceback.print_exc()
            status = "failed"
        finally:
            try:
                log_run(conn, task_id, "wos", query, total_found, total_new, status)
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass

    try:
        conn.close()
    except Exception:
        pass

    print(f"\n{'=' * 60}")
    print(f"  WoS 爬取完毕")
    print(f"  状态: {status} | 模式: {mode}")
    print(f"  总结果数: {total_found}")
    print(f"  本次新增: {total_new}")
    print(f"{'=' * 60}\n")
    return status


# ============================================================
#  独立运行入口
# ============================================================

if __name__ == "__main__":
    asyncio.run(scrape_wos(
        task_id=1,
        query="TS=(artificial intelligence AND healthcare)",
        mode="quick",      # "quick" 或 "deep"
        max_records=5,
        show_browser=True,
    ))
