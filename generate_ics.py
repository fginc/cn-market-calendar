from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, date

import pandas as pd
import pytz
from dateutil.relativedelta import relativedelta
from icalendar import Calendar, Event

# 时区
TZ = pytz.timezone("Asia/Shanghai")

# 未来多少天（可在 GitHub Actions 里用 env DAYS_FORWARD 覆盖）
DAYS_FORWARD = int(os.getenv("DAYS_FORWARD", "90"))

# 输出目录：GitHub Actions 会把 public/ 发布到 Pages
OUT_DIR = "public"
UNLOCK_MV_MIN_YI = float(os.getenv("UNLOCK_MV_MIN_YI", "5"))  # 解禁市值阈值：亿元
MAX_EVENTS_PER_DAY = int(os.getenv("MAX_EVENTS_PER_DAY", "30"))  # 同一天最多保留多少条（避免刷屏）
os.makedirs(OUT_DIR, exist_ok=True)


def _to_dt(x) -> datetime | None:
    """把各种日期格式安全转成 datetime（失败返回 None）"""
    if x is None or (isinstance(x, float) and pd.isna(x)) or (hasattr(pd, "isna") and pd.isna(x)):
        return None
    try:
        ts = pd.to_datetime(x)
        if pd.isna(ts):
            return None
        if isinstance(ts, pd.Timestamp):
            ts = ts.to_pydatetime()
        if isinstance(ts, date) and not isinstance(ts, datetime):
            ts = datetime(ts.year, ts.month, ts.day)
        return ts
    except Exception:
        return None


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """从 DataFrame 中挑选匹配列名（精确+模糊）"""
    cols = list(df.columns)
    for c in candidates:
        if c in cols:
            return c

    def norm(s: str) -> str:
        return re.sub(r"[\s\(\)（）_\-]", "", str(s))

    ncols = {norm(c): c for c in cols}
    for c in candidates:
        nc = norm(c)
        if nc in ncols:
            return ncols[nc]
    return None


def make_cal(name: str) -> Calendar:
    cal = Calendar()
    cal.add("prodid", f"-//{name}//CN Market Calendar//")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")
    cal.add("X-WR-CALNAME", name)          # 苹果日历显示名
    cal.add("X-WR-TIMEZONE", "Asia/Shanghai")
    return cal


def add_all_day_event(cals, day: date, summary: str, description: str = "", uid: str = ""):
    """
    同一个事件写入多个 Calendar（例如分类日历 + 总日历）
    关键：每个日历都重新创建一个 Event（避免出现嵌套 VEVENT）
    """
    if not isinstance(cals, (list, tuple)):
        cals = [cals]

    for cal in cals:
        ev = Event()
        ev.add("summary", summary)
        ev.add("dtstart", day)
        ev.add("dtend", day + timedelta(days=1))
        if description:
            ev.add("description", description)
        ev.add("uid", uid or f"{summary}-{day.isoformat()}@cn-market-calendar")
        ev.add("dtstamp", datetime.now(tz=TZ))
        cal.add_component(ev)


def write_ics(cal: Calendar, filename: str):
    path = os.path.join(OUT_DIR, filename)
    with open(path, "wb") as f:
        f.write(cal.to_ical())
    print("Wrote:", path)


def date_range() -> tuple[date, date]:
    start = datetime.now(tz=TZ).date()
    end = (datetime.now(tz=TZ) + timedelta(days=DAYS_FORWARD)).date()
    return start, end


# --------------------------
# 01 新股：申购 / 缴款 / 上市
# --------------------------
def gen_ipo_calendar(cal_all: Calendar):
    import akshare as ak

    start, end = date_range()
    cal = make_cal("A股｜新股申购/缴款/上市")

    df = ak.stock_xgsglb_em()

    code_col = _pick_col(df, ["股票代码", "申购代码"])
    name_col = _pick_col(df, ["股票简称"])
    apply_col = _pick_col(df, ["申购日期"])
    pay_col = _pick_col(df, ["中签缴款日期", "网上申购缴款日"])
    list_col = _pick_col(df, ["上市日期", "上市日"])

    def in_range(d: datetime | None) -> bool:
        if not d:
            return False
        dd = d.date()
        return start <= dd <= end

    for _, r in df.iterrows():
        code = str(r.get(code_col, "")).strip() if code_col else ""
        nm = str(r.get(name_col, "")).strip() if name_col else ""
        title = f"{nm}({code})" if code and nm else (nm or code or "新股")

        d_apply = _to_dt(r.get(apply_col)) if apply_col else None
        d_pay = _to_dt(r.get(pay_col)) if pay_col else None
        d_list = _to_dt(r.get(list_col)) if list_col else None

        if in_range(d_apply):
            add_all_day_event([cal, cal_all], d_apply.date(), f"新股申购｜{title}", uid=f"ipo-apply-{code}-{d_apply.date()}")
        if in_range(d_pay):
            add_all_day_event([cal, cal_all], d_pay.date(), f"中签缴款｜{title}", uid=f"ipo-pay-{code}-{d_pay.date()}")
        if in_range(d_list):
            add_all_day_event([cal, cal_all], d_list.date(), f"新股上市｜{title}", uid=f"ipo-list-{code}-{d_list.date()}")

    write_ics(cal, "01_ipo.ics")


# --------------------------
# 02 解禁：限售解禁日历
# --------------------------
def gen_unlock_calendar(cal_all: Calendar):
    import akshare as ak

    start, end = date_range()
    cal = make_cal("A股｜限售解禁")

    df = None
    tried = []

    # AkShare 版本差异很大：这里做“多接口尝试”
    candidates = [
        "stock_restricted_release_queue_em",
        "stock_restricted_release_summary_em",
        "stock_restricted_release_detail_em",
        "stock_restricted_release_em",
    ]
    for fn in candidates:
        if hasattr(ak, fn):
            tried.append(fn)
            try:
                df = getattr(ak, fn)()
                break
            except Exception:
                df = None

    if df is None:
        raise RuntimeError(
            f"未找到可用的全市场解禁接口（已尝试：{tried}）。"
            f"请升级 akshare 或把你本地可用的解禁函数名发我。"
        )

    date_col = _pick_col(df, ["解禁日期", "日期"])
    code_col = _pick_col(df, ["股票代码", "代码"])
    name_col = _pick_col(df, ["股票简称", "名称"])
    amt_col = _pick_col(df, ["解禁数量", "解禁股数", "数量", "解禁数量(万股)"])
    mv_col = _pick_col(df, ["解禁市值", "市值", "解禁市值(亿元)"])

    for _, r in df.iterrows():
        d = _to_dt(r.get(date_col)) if date_col else None
        if not d:
            continue
        dd = d.date()
        if not (start <= dd <= end):
            continue

        code = str(r.get(code_col, "")).strip() if code_col else ""
        nm = str(r.get(name_col, "")).strip() if name_col else ""
        amt = str(r.get(amt_col, "")).strip() if amt_col else ""
        mv = str(r.get(mv_col, "")).strip() if mv_col else ""

        title = f"{nm}({code})" if code and nm else (nm or code or "解禁")
        desc = "；".join([x for x in [
            f"解禁数量: {amt}" if amt else "",
            f"解禁市值: {mv}" if mv else ""
        ] if x])

                # 过滤：只保留“解禁市值 >= 阈值”的大解禁
        mv_yi = None
        if mv:
            try:
                mv_yi = float(str(mv).replace(",", "").replace("亿", "").strip())
            except Exception:
                mv_yi = None

        if mv_yi is not None and mv_yi < UNLOCK_MV_MIN_YI:
            continue

        add_all_day_event([cal, cal_all], dd, f"限售解禁｜{title}", description=desc, uid=f"unlock-{code}-{dd}")

    write_ics(cal, "02_unlock.ics")


# --------------------------
# 03 财报：预约/实际披露
# --------------------------
def gen_earnings_calendar(cal_all: Calendar):
    import akshare as ak

    start, end = date_range()
    cal = make_cal("A股｜财报披露（预约）")

    df = ak.stock_yysj_em()

    code_col = _pick_col(df, ["股票代码", "代码"])
    name_col = _pick_col(df, ["股票简称", "名称"])
    first_col = _pick_col(df, ["首次预约", "首次预约披露", "首次预约时间"])
    actual_col = _pick_col(df, ["实际披露", "实际披露时间"])
    report_col = _pick_col(df, ["报告期", "报告期别", "报告期类型"])

    for _, r in df.iterrows():
        code = str(r.get(code_col, "")).strip() if code_col else ""
        nm = str(r.get(name_col, "")).strip() if name_col else ""
        rp = str(r.get(report_col, "")).strip() if report_col else ""
        title = f"{nm}({code})" if code and nm else (nm or code or "财报")

        d = _to_dt(r.get(actual_col)) if actual_col else None
        if not d and first_col:
            d = _to_dt(r.get(first_col))
        if not d:
            continue

        dd = d.date()
        if not (start <= dd <= end):
            continue

        add_all_day_event(
            [cal, cal_all],
            dd,
            f"财报披露｜{title}" + (f"｜{rp}" if rp else ""),
            uid=f"earn-{code}-{dd}"
        )

    write_ics(cal, "03_earnings.ics")


# --------------------------
# 04 分红/除权除息
# --------------------------
def gen_dividend_calendar(cal_all: Calendar):
    import akshare as ak

    start, end = date_range()
    cal = make_cal("A股｜分红/除权除息")

    # 不同版本的 AkShare 分红接口不同，这里做一次兜底
    if hasattr(ak, "stock_fhps_em"):
        df = ak.stock_fhps_em()
    elif hasattr(ak, "stock_fhps_detail_em"):
        df = ak.stock_fhps_detail_em()
    else:
        raise RuntimeError(
            "你本地 akshare 缺少分红接口（stock_fhps_em / stock_fhps_detail_em）。"
            "请升级 akshare，或把你本地可用的分红函数名发我。"
        )

    date_col = _pick_col(df, ["除权除息日", "除息日", "权益登记日", "日期"])
    code_col = _pick_col(df, ["代码", "股票代码"])
    name_col = _pick_col(df, ["名称", "股票简称"])
    plan_col = _pick_col(df, ["分红方案", "方案", "送转派", "派息方案"])

    for _, r in df.iterrows():
        d = _to_dt(r.get(date_col)) if date_col else None
        if not d:
            continue
        dd = d.date()
        if not (start <= dd <= end):
            continue

        code = str(r.get(code_col, "")).strip() if code_col else ""
        nm = str(r.get(name_col, "")).strip() if name_col else ""
        plan = str(r.get(plan_col, "")).strip() if plan_col else ""
        title = f"{nm}({code})" if code and nm else (nm or code or "分红")

        add_all_day_event(
            [cal, cal_all],
            dd,
            f"分红/除权除息｜{title}",
            description=plan,
            uid=f"div-{code}-{dd}"
        )

    write_ics(cal, "04_dividend.ics")


# --------------------------
# 05 指数调样（规则日）
# --------------------------
def gen_index_rebalance_calendar(cal_all: Calendar):
    # 规则日历：3/6/9/12 月第二个周五（通常生效为下一交易日，以公告为准）
    start, end = date_range()
    cal = make_cal("A股｜指数调样（规则日）")

    def second_friday(y: int, m: int) -> date:
        d = date(y, m, 1)
        while d.weekday() != 4:  # Friday=4
            d += timedelta(days=1)
        return d + timedelta(days=7)

    cursor = start.replace(day=1)
    while cursor <= end:
        if cursor.month in (3, 6, 9, 12):
            d = second_friday(cursor.year, cursor.month)
            if start <= d <= end:
                add_all_day_event(
                    [cal, cal_all],
                    d,
                    "指数样本定期调整窗口（按规则推算；最终以公告为准）",
                    uid=f"idx-reb-{d}"
                )
        cursor = (cursor + relativedelta(months=1)).replace(day=1)

    write_ics(cal, "05_index_rebalance_rules.ics")


# --------------------------
# 06 宏观数据/事件（日历源：华尔街见闻宏观日历）
# --------------------------
def gen_macro_calendar(cal_all: Calendar):
    import akshare as ak

    start, end = date_range()
    cal = make_cal("宏观｜重要经济数据/事件")

    if not hasattr(ak, "macro_info_ws"):
        raise RuntimeError("你本地 akshare 缺少宏观日历接口 macro_info_ws，请升级 akshare。")

    df = ak.macro_info_ws()

    time_col = _pick_col(df, ["时间", "date", "datetime"])
    country_col = _pick_col(df, ["国家", "country"])
    event_col = _pick_col(df, ["事件", "event", "指标"])
    imp_col = _pick_col(df, ["重要性", "importance"])
    exp_col = _pick_col(df, ["预期", "forecast"])
    pre_col = _pick_col(df, ["前值", "previous"])

    for _, r in df.iterrows():
        dtt = _to_dt(r.get(time_col)) if time_col else None
        if not dtt:
            continue
        dd = dtt.date()
        if not (start <= dd <= end):
            continue

        ctry = str(r.get(country_col, "")).strip() if country_col else ""
        evn = str(r.get(event_col, "")).strip() if event_col else ""
        imp = str(r.get(imp_col, "")).strip() if imp_col else ""
        exp = str(r.get(exp_col, "")).strip() if exp_col else ""
        pre = str(r.get(pre_col, "")).strip() if pre_col else ""

                # 去噪：只保留重点国家（可按需加/减）
        if ctry and ctry not in ("中国", "美国", "欧元区"):
            continue

        # 去噪：只保留高重要性（不同数据源格式不一，尽量兼容）
        if imp:
            imp_s = str(imp)
            # 常见：★★★★★ / 3 / 高 / 重要 / ★★★ 等
            if ("高" in imp_s) or ("重要" in imp_s):
                pass
            else:
                # 提取数字重要性（如 1/2/3/4/5）
                nums = re.findall(r"\d+", imp_s)
                if nums:
                    try:
                        if int(nums[0]) < 3:
                            continue
                    except Exception:
                        pass
                elif "★" in imp_s:
                    if imp_s.count("★") < 3:
                        continue
                else:
                    # 不认识的格式：不过滤
                    pass

        summary = f"{ctry}｜{evn}" if ctry else evn
        desc = "；".join([x for x in [
            f"重要性: {imp}" if imp else "",
            f"预期: {exp}" if exp else "",
            f"前值: {pre}" if pre else ""
        ] if x])

        add_all_day_event(
            [cal, cal_all],
            dd,
            f"宏观数据｜{summary}",
            description=desc,
            uid=f"macro-{dd}-{abs(hash(summary))}"
        )

    write_ics(cal, "06_macro.ics")

def gen_cn_report_deadlines_template(cal_all: Calendar):
    """A股财报披露硬截止日 + 常见密集窗口（规则/经验层）"""
    start, end = date_range()
    cal = make_cal("模板｜财报季与窗口（规则）")

    y = start.year
    # 覆盖未来一年多一点
    years = {y, y + 1}

    for yy in sorted(years):
        fixed_days = [
            (date(yy, 4, 30), "A股｜一季报披露截止日（通常4/30）"),
            (date(yy, 4, 30), "A股｜年报披露截止日（通常4/30）"),
            (date(yy, 8, 31), "A股｜中报披露截止日（通常8/31）"),
            (date(yy, 10, 31), "A股｜三季报披露截止日（通常10/31）"),
        ]
        for d, s in fixed_days:
            if start <= d <= end:
                add_all_day_event([cal, cal_all], d, s, uid=f"tpl-report-deadline-{s}-{d}")

        # 经验窗口：用“周”做区间（全是全天事件，不会误导到具体时刻）
        windows = [
            (date(yy, 1, 10), date(yy, 1, 31), "A股｜年报预告/快报密集窗口（经验）"),
            (date(yy, 4, 1), date(yy, 4, 30), "A股｜财报披露高峰（月度窗口）"),
            (date(yy, 7, 1), date(yy, 7, 31), "A股｜中报预告密集窗口（经验）"),
            (date(yy, 8, 1), date(yy, 8, 31), "A股｜中报披露高峰（月度窗口）"),
            (date(yy, 10, 1), date(yy, 10, 31), "A股｜三季报披露高峰（月度窗口）"),
        ]
        for d1, d2, s in windows:
            # 用开始日标记窗口即可（不做每日重复，避免刷屏）
            if start <= d1 <= end:
                add_all_day_event([cal, cal_all], d1, s, description=f"窗口范围：{d1} ~ {d2}", uid=f"tpl-window-{s}-{d1}")

    write_ics(cal, "07_report_templates.ics")


def gen_cn_macro_template(cal_all: Calendar):
    """中国宏观数据发布时间“常见窗口”（规则/经验层）"""
    start, end = date_range()
    cal = make_cal("模板｜中国宏观数据窗口（经验）")

    # 生成未来 N 个月的“窗口提示”
    cursor = date(start.year, start.month, 1)

    while cursor <= end:
        yy, mm = cursor.year, cursor.month

        # 1) 外汇储备：常见在每月上旬（这里用每月第 7 日作为提醒点）
        d_fx = date(yy, mm, 7)
        if start <= d_fx <= end:
            add_all_day_event([cal, cal_all], d_fx, f"宏观｜外汇储备公布窗口（经验：上旬）", uid=f"tpl-macro-fx-{d_fx}")

        # 2) CPI/PPI：常见在每月上旬（这里用第 10 日）
        d_cpi = date(yy, mm, 10)
        if start <= d_cpi <= end:
            add_all_day_event([cal, cal_all], d_cpi, f"宏观｜CPI/PPI公布窗口（经验：上旬）", uid=f"tpl-macro-cpi-{d_cpi}")

        # 3) 社融/信贷/M2：常见在每月中旬（这里用第 15 日）
        d_ts = date(yy, mm, 15)
        if start <= d_ts <= end:
            add_all_day_event([cal, cal_all], d_ts, f"宏观｜社融/信贷/M2公布窗口（经验：中旬）", uid=f"tpl-macro-ts-{d_ts}")

        # 4) LPR：每月 20 日（相对固定）
        d_lpr = date(yy, mm, 20)
        if start <= d_lpr <= end:
            add_all_day_event([cal, cal_all], d_lpr, f"宏观｜LPR报价日（每月20日）", uid=f"tpl-macro-lpr-{d_lpr}")

        # 5) PMI：常见在月末（这里用每月最后一天）
        # 计算当月最后一天
        next_month = (cursor + relativedelta(months=1))
        last_day = next_month - timedelta(days=1)
        if start <= last_day <= end:
            add_all_day_event([cal, cal_all], last_day, f"宏观｜PMI公布窗口（月末/次月初附近）", uid=f"tpl-macro-pmi-{last_day}")

        cursor = cursor + relativedelta(months=1)

    write_ics(cal, "08_macro_templates.ics")

def gen_nbs_release_calendar(cal_all: Calendar):
    """
    国家统计局：最新统计信息发布日程（每年更新一次）
    来源：https://www.stats.gov.cn/sj/fbrc/bnxxfb/
    生成：09_nbs_release.ics，并写入 00_all.ics
    """
    import requests
    from bs4 import BeautifulSoup

    url = "https://www.stats.gov.cn/sj/fbrc/bnxxfb/"
    cal = make_cal("国家统计局｜重要数据发布日程")

    # 1) 拉取网页
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; github-actions; +https://github.com/)"
    }
    resp = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
    resp.raise_for_status()
# 编码兜底（避免中文变成乱码导致匹配不到“2026年”）
    resp.encoding = resp.apparent_encoding or resp.encoding or "utf-8"
    html = resp.text

    soup = BeautifulSoup(html, "lxml")

    # 2) 从页面标题/正文中抓年份（例如：2026年国家统计局主要统计信息发布日程表）
    # 用更稳妥的方式拿到文本与年份
    text_all = soup.get_text("\n", strip=True)

    # 1) 尝试从全文找所有“XXXX年”，取最大值（通常就是当年日程）
    years = [int(y) for y in re.findall(r"(\d{4})\s*年", text_all)]
    if years:
        year = max(years)
    else:
        # 2) 如果页面里完全找不到年份（可能被反爬/返回模板页），用当前年份兜底，不让流程挂
        year = datetime.now(tz=TZ).year

    # 3) 找到主表格（页面里通常只有一个核心日程表，选包含“序号/内容/1月”的表）
    tables = soup.find_all("table")
    target = None
    for t in tables:
        t_text = t.get_text(" ", strip=True)
        if ("序号" in t_text) and ("内容" in t_text) and ("1月" in t_text) and ("12月" in t_text):
            target = t
            break
    if target is None:
        raise RuntimeError("未找到日程表格（页面结构可能变了）")

    # 4) 读表格：逐行取单元格文本
    rows = []
    for tr in target.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
        if cells:
            rows.append(cells)

    # 5) 识别表头：找到月份列起点
    # 期望表头里包含：序号、内容、1月..12月
    header_idx = None
    for i, r in enumerate(rows[:10]):  # 表头一般在前几行
        joined = " ".join(r)
        if ("序号" in joined) and ("内容" in joined) and ("1月" in joined) and ("12月" in joined):
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError("未识别到表头行（页面结构可能变了）")

    header = rows[header_idx]
    # 找到“1月”所在列
    try:
        m_start = header.index("1月")
    except ValueError:
        # 兜底：找包含“1月”的单元格
        m_start = next(i for i, v in enumerate(header) if "1月" in v)

    # 月份列 1..12
    month_cols = list(range(m_start, m_start + 12))

    # 6) 逐条内容解析：通常是“日期行” + 紧跟一个“时间行”
    # 日期行：内容列有文字，月份列里是“19/一”“4/三 注5”“……”等
    # 时间行：内容列为空/很短，月份列里是“10:00”“9:30”重复
    def parse_day(cell: str) -> int | None:
        if not cell:
            return None
        if "……" in cell or cell in ("-", "—"):
            return None
        # 取开头的数字（例如 "4/三 注5" -> 4）
        mday = re.match(r"^\s*(\d{1,2})\s*/", cell)
        if not mday:
            return None
        return int(mday.group(1))

    def parse_time(cell: str) -> tuple[int, int] | None:
        if not cell:
            return None
        mt = re.search(r"(\d{1,2}):(\d{2})", cell)
        if not mt:
            return None
        return int(mt.group(1)), int(mt.group(2))

    content_col = None
    # 找“内容”列
    for i, v in enumerate(header):
        if "内容" == v or "内容" in v:
            content_col = i
            break
    if content_col is None:
        content_col = 1  # 常见结构：第2列

    # 从表头下一行开始遍历
    i = header_idx + 1
    while i < len(rows):
        r = rows[i]
        # 防止短行
        if len(r) <= max(month_cols + [content_col]):
            i += 1
            continue

        content = r[content_col].strip()

        # 判断是不是“日期行”
        has_days = any(parse_day(r[c]) is not None for c in month_cols)
        has_times = any(parse_time(r[c]) is not None for c in month_cols)

        # 日期行：有内容 + 有day
        if content and has_days:
            # 先收集所有月份的日期
            day_map: dict[int, int] = {}  # month -> day
            for mi, col in enumerate(month_cols, start=1):
                d = parse_day(r[col])
                if d is not None:
                    day_map[mi] = d

            # 看下一行是否是时间行
            time_map: dict[int, tuple[int, int]] = {}
            if i + 1 < len(rows):
                r2 = rows[i + 1]
                if len(r2) > max(month_cols + [content_col]):
                    content2 = r2[content_col].strip()
                    has_times2 = any(parse_time(r2[c]) is not None for c in month_cols)
                    # 时间行：内容空/很短 + 有time
                    if (not content2 or content2 in ("", " ", "　")) and has_times2:
                        for mi, col in enumerate(month_cols, start=1):
                            t = parse_time(r2[col])
                            if t is not None:
                                time_map[mi] = t
                        i += 1  # 吃掉下一行

            # 默认时间（如果时间行没给某个月，就尝试用第一个时间作为默认）
            default_time = None
            if time_map:
                default_time = next(iter(time_map.values()))

            # 生成事件
            for month, day in day_map.items():
                hhmm = time_map.get(month) or default_time
                if hhmm:
                    hh, mm = hhmm
                    dt = TZ.localize(datetime(year, month, day, hh, mm))
                    # timed event：给1小时默认时长（只为日历显示好看）
                    ev = Event()
                    ev.add("summary", f"国家统计局｜{content}")
                    ev.add("dtstart", dt)
                    ev.add("dtend", dt + timedelta(hours=1))
                    ev.add("uid", f"nbs-{year}-{month:02d}-{day:02d}-{abs(hash(content))}@stats.gov.cn")
                    ev.add("dtstamp", datetime.now(tz=TZ))
                    ev.add("description", f"来源：{url}\n注：发布日期为初步计划，可能调整。")
                    cal.add_component(ev)
                    cal_all.add_component(ev)
                else:
                    # 如果页面没给时间，就做全天事件
                    add_all_day_event([cal, cal_all], date(year, month, day), f"国家统计局｜{content}",
                                      description=f"来源：{url}\n注：发布日期为初步计划，可能调整。",
                                      uid=f"nbs-{year}-{month:02d}-{day:02d}-{abs(hash(content))}@stats.gov.cn")

        i += 1

    write_ics(cal, "09_nbs_release.ics")

if __name__ == "__main__":
    # 总合集（日历订阅只需要这一个链接）
    cal_all = make_cal("中国市场投资日历（全量）")

    gen_ipo_calendar(cal_all)
    gen_unlock_calendar(cal_all)
    gen_earnings_calendar(cal_all)
    gen_dividend_calendar(cal_all)
    gen_index_rebalance_calendar(cal_all)
    gen_macro_calendar(cal_all)
    gen_cn_report_deadlines_template(cal_all)
    gen_cn_macro_template(cal_all)
    try:
        gen_nbs_release_calendar(cal_all)
    except Exception as e:
        print("NBS calendar skipped due to error:", repr(e))

    # 输出总合集 + 分主题
    write_ics(cal_all, "00_all.ics")
