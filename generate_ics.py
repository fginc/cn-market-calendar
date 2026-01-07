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
os.makedirs(OUT_DIR, exist_ok=True)


def _to_dt(x) -> datetime | None:
    """把各种日期格式安全转成 datetime（失败返回 None）"""
    if pd.isna(x) or x is None:
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
    cal.add("X-WR-CALNAME", name)         # 苹果日历显示名
    cal.add("X-WR-TIMEZONE", "Asia/Shanghai")
    return cal


def add_all_day_event(cals, day: date, summary: str, description: str = "", uid: str = ""):
    """
    同一个事件写入多个 Calendar（例如分类日历 + 总日历）
    """
    if not isinstance(cals, (list, tuple)):
        cals = [cals]

    ev = Event()
    ev.add("summary", summary)
    ev.add("dtstart", day)
    ev.add("dtend", day + timedelta(days=1))
    if description:
        ev.add("description", description)
    ev.add("uid", uid or f"{summary}-{day.isoformat()}@cn-market-calendar")
    ev.add("dtstamp", datetime.now(tz=TZ))

    # 保险做法：为每个日历 clone 一份 Event
    for cal in cals:
        ev2 = Event()
        for k, v in ev.property_items():
            ev2.add(k, v)
        cal.add_component(ev2)


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

    df = ak.stock_xgsglb_em()  # 新股申购与中签查询

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

        add_all_day_event([cal, cal_all], dd, f"限售解禁｜{title}", description=desc, uid=f"unlock-{code}-{dd}")

    write_ics(cal, "02_unlock.ics")


# --------------------------
# 03 财报：预约/实际披露
# --------------------------
def gen_earnings_calendar(cal_all: Calendar):
    import akshare as ak

    start, end = date_range()
    cal = make_cal("A股｜财报披露（预约）")

    df = ak.stock_yysj_em()  # 预约披露时间

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


if __name__ == "__main__":
    # 总合集（日历订阅只需要这一个链接）
    cal_all = make_cal("中国市场投资日历（全量）")

    gen_ipo_calendar(cal_all)
    gen_unlock_calendar(cal_all)
    gen_earnings_calendar(cal_all)
    gen_dividend_calendar(cal_all)
    gen_index_rebalance_calendar(cal_all)
    gen_macro_calendar(cal_all)

    # 输出总合集
    write_ics(cal_all, "00_all.ics")
