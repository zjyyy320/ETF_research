from WindPy import w
import pandas_datareader.data as web
import pandas as pd
import os
import numpy as np
import warnings

pd.options.mode.chained_assignment = None
warnings.filterwarnings('ignore', category=FutureWarning)


# ===================== 配置 =====================
NEW_DATE="2026-2-21"
CONFIG = {
    "end_date": "2026-02-05",
    "long_path": r"/Users/zjy/python/ETF/ETF跟踪指数量价数据-非日度更新/",
    "short_path": r"/Users/zjy/python/ETF/ETF跟踪指数量价数据-日度更新/",
    "wind_params": {
        "Days": "Trading",        # 仅抓取交易日数据
        "Fill": "Blank",          # 空值（代码层不额外处理）
        "Order": "D",             # 数据按日期降序返回
        "Period": "D",            # 日频数据
        "TradingCalendar": "SSE", # 以上交所交易日历为准
        "Currency": "Original",   # 原始货币单位
        "PriceAdj": "U"           # 价格不复权Calendar": "SSE", "Currency": "Original", "PriceAdj": "U"
    },
    "symbols": {
    # "980092.CNI": "2013-01-02",    # 自由现金流指数
    # "h30269.CSI": "2018-05-11",    # 红利低波指数
    # "000905.SH": "2005-01-04",     # 中证500指数
    # "932000.CSI": "2014-01-02",    # 中证2000指数
    # "000688.SH": "2020-01-02",     # 科创50指数
    "000300.SH": "2005-01-04",     # 沪深300指数
    # "HSHYLV.HI": "2017-05-22",     # 恒生港股通高股息低波动指数
    # "HSBIO.HI": "2019-12-16",      # 恒生生物科技指数
    # "HSTECH.HI": "2020-07-27",     # 恒生科技指数
    # "930709.CSI": "2020-02-24",    # 香港证券指数
    # # "DJI.GI": "1997-11-07",        # 道琼斯工业平均指数
    # "000201.CZC": "2010-03-02",    # 易盛能化A指数
    # "NH0700.NHF": "2018-06-29",    # 南华有色金属指数
    # "NH0015.NHF": "2004-06-01",    # 南华豆粕指数
    # "AU.SHF": "2008-01-09",        # SHFE黄金期货
    # "511380.SH": "2020-04-07",     # 可转债ETF（博时）
    # "511020.SH": "2019-02-22",     # 国债ETF（5至10年）
    # "511260.SH": "2017-08-24",     # 十年国债ETF
    # "511220.SH": "2014-12-16",     # 城投债ETF（海富通）
    # "159972.SZ": "2019-11-08",     # 5年地债ETF
    # "159816.SZ": "2020-09-04"      # 0-4年地债ETF
    }
}

EXTERNAL_SYMBOLS = {
    "^DJI": "2006-01-04",      # 道琼斯工业指数
    "^SPX": "2006-01-04",      # S&P 500 指数
    "^NKX": "2006-01-04",      # 日经225指数
    "1321.JP": "2007-07-30",   # 日经225 ETF (野村)
    "^DAX": "2006-01-04",      # 德国DAX指数
    "EXS1.DE": "2007-01-04"    # 德国DAX ETF (iShares)
}

# ===================== 工具函数 =====================
def _wind_opts(params: dict) -> str:
    """生成Wind API参数字符串"""
    return ";".join(f"{k}={v}" for k, v in params.items())

def _save_df(df: pd.DataFrame, symbol: str, path: str):
    """保存DataFrame为CSV（自动创建目录）"""
    os.makedirs(path, exist_ok=True)  # ← 直接内联
    fp = os.path.join(path, f"{symbol}.csv")
    df.fillna("").to_csv(fp, index=False, encoding="utf-8-sig")
    print(f"✅ {symbol}: 已保存至 {fp}")

def _read_latest_date(symbol: str, path: str) -> str:
    """从已有CSV读取wind最新日期（用于增量起点）"""
    fp = os.path.join(path, f"{symbol}.csv")
    if not os.path.exists(fp):
        return CONFIG["symbols"][symbol]
    try:
        date_str = pd.read_csv(fp, usecols=["date"], nrows=1).iloc[0]["date"]
        return pd.to_datetime(date_str).strftime("%Y-%m-%d")
    except Exception:
        return CONFIG["symbols"][symbol]
    
def _read_external_latest_date(symbol: str, path: str) -> str:
    """从已有CSV读取海外标的最新日期（用于增量起点）"""
    fp = os.path.join(path, f"{symbol}.csv")
    if not os.path.exists(fp):
        return EXTERNAL_SYMBOLS[symbol]
    try:
        date_str = pd.read_csv(fp, usecols=["date"], nrows=1).iloc[0]["date"]
        return pd.to_datetime(date_str).strftime("%Y-%m-%d")
    except Exception:
        return EXTERNAL_SYMBOLS[symbol]
    

def _align_to_target_dates(raw_df: pd.DataFrame, symbol: str, target_dates: pd.Series, start_date: str) -> pd.DataFrame:
    """
    将 raw_df 按照 target_dates（000300.SH 的 date 列）对齐
    - 只保留 >= start_date 的日期
    - 缺失日期用空值填充（保持结构）
    """
    # 确保 raw_df 的 date 是字符串（与 target 一致）
    raw_df = raw_df.copy()
    raw_df["date"] = pd.to_datetime(raw_df["date"])
    target_dates_dt = pd.to_datetime(target_dates)

    # 筛选 target 中 >= start_date 的日期
    aligned_dates = target_dates_dt[target_dates_dt >= pd.to_datetime(start_date)].copy()

    # 构建完整日期框架
    aligned_df = pd.DataFrame({"date": aligned_dates})
    
    # 左连接原始数据（保留所有目标日期，缺失填空）
    aligned_df = aligned_df.merge(raw_df, on="date", how="left")

    # 转回字符串格式
    aligned_df["date"] = aligned_df["date"].dt.strftime("%Y-%m-%d")
    
    return aligned_df

def _reprocess_nulls_for_aligned(df: pd.DataFrame, end: str) -> pd.DataFrame:
    """
    对已对齐的 DataFrame 重新执行空值处理：
    - 删除 end 当天的空值行
    - 对 end 之前的空值用前一个交易日填充（注意：df 是降序！）
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    end_date = pd.to_datetime(end).date()

    price_cols = ["open", "close", "high", "low"]
    vol_col = "volume"
    all_cols = price_cols + [vol_col]

    # 标记空值行
    mask_null = df[all_cols].isnull().any(axis=1)

    # 删除 end 当天的空值行
    is_end_day = df["date"].dt.date == end_date
    to_delete = mask_null & is_end_day
    if to_delete.any():
        deleted_dates = df[to_delete]["date"].dt.strftime("%Y-%m-%d").unique()
        df = df[~to_delete].copy()
        for d in deleted_dates:
            print(f"⚠️ 对齐后删除: {d} 含空值（end_date）")

    if df.empty:
        return df

    # 注意：df 是降序（最新在前），所以要用 bfill（用下一行填充当前空值 = 用更早的交易日填充）
    was_null = df[all_cols].isnull().any(axis=1)
    df[all_cols] = df[all_cols].fillna(method='bfill')
    now_valid = ~df[all_cols].isnull().any(axis=1)
    filled_mask = was_null & now_valid
    if filled_mask.any():
        filled_dates = df[filled_mask]["date"].dt.strftime("%Y-%m-%d").unique()
        # for d in filled_dates:
        #     print(f"⚠️ 对齐后填充: {d} 空值已用前一日数据补充")

    # 格式标准化（确保字符串格式）
    for col in price_cols:
        df[col] = df[col].astype(float).apply(lambda x: f"{x:.4f}")
    df["volume"] = (
        df["volume"]
        .astype(float)
        .round()
        .fillna(0)
        .astype(int)
        .astype(str)
    )
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df

# ===================== 数据抓取与清洗 =====================
def _fetch_clean(symbol: str, start: str, end: str, params: dict) -> pd.DataFrame | None:
    """从Wind获取并清洗量价数据（open/close/high/low/volume）"""
    raw = w.wsd(symbol, "open,close,high,low,volume", start, end, _wind_opts(params))
    if raw.ErrorCode != 0 or not raw.Data:
        print(f"⚠️ {symbol}: 无有效数据")
        return None

    df = pd.DataFrame(raw.Data, index=["open","close","high","low","volume"], columns=raw.Times).T
    df.reset_index(inplace=True)
    df.columns = ["date", "open", "close", "high", "low", "volume"]
    df["date"] = pd.to_datetime(df["date"])
    df.replace([np.inf, -np.inf], np.nan, inplace=True)


# 标记含空值的行
    price_cols = ["open", "close", "high", "low"]
    vol_col = "volume"
    # 标记含空值的行
    mask_null = df[price_cols + [vol_col]].isnull().any(axis=1)
    if mask_null.any():
        end_date = pd.to_datetime(end).date()
        is_end_day = df["date"].dt.date == end_date

        # 删除 end 当天的空值行
        to_delete = mask_null & is_end_day
        if to_delete.any():
            deleted_dates = df[to_delete]["date"].dt.strftime("%Y-%m-%d").unique()
            df = df[~to_delete].copy()
            for d in deleted_dates:
                print(f"⚠️ {symbol}: {d} 含空值，已删除此行")

        # 关键：Wind 是降序 → 用 bfill 实现“用前一个交易日（时间上更早）填充”
        if not df.empty:
            was_null = df[price_cols + [vol_col]].isnull().any(axis=1)
            # 在降序数据中，bfill = 用下一行（时间更早）填充当前空值
            df[price_cols + [vol_col]] = df[price_cols + [vol_col]].fillna(method='bfill')
            
            # 检查哪些空值被成功填充
            now_valid = ~df[price_cols + [vol_col]].isnull().any(axis=1)
            filled_mask = was_null & now_valid
            if filled_mask.any():
                filled_dates = df[filled_mask]["date"].dt.strftime("%Y-%m-%d").unique()
                for d in filled_dates:
                    print(f"⚠️ {symbol}: {d} 含空值，已补充为前一日数据")

    if df.empty:
        return None

    # 格式标准化：价格4位小数，成交量整数
    for col in ["open","close","high","low"]:
        df[col] = df[col].astype(float).apply(lambda x: f"{x:.4f}")

    # 然后再执行转换
    df["volume"] = df["volume"].astype(float).round().astype(int).astype(str)
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    print(f"✅ {symbol}: 抓取 {len(df)} 条有效数据")
    return df

def _fetch_clean_stooq(name: str, symbol: str, start: str, end: str) -> pd.DataFrame | None:
    """
    从 Stooq 获取并清洗海外指数/ETF 数据，仅做基础清洗，保留 NaN，不转字符串。
    所有格式化和空值处理由 _reprocess_nulls_for_aligned 统一执行。
    """
    try:
        df = web.DataReader(symbol, 'stooq', start, end)
    except Exception as e:
        print(f"⚠️ {name} ({symbol}): Stooq 下载失败 - {e}")
        return None

    if df.empty:
        print(f"⚠️ {name} ({symbol}): 无数据返回")
        return None

    df.columns = [col.lower() for col in df.columns]
    required_cols = ["open", "close", "high", "low", "volume"]
    if not all(col in df.columns for col in required_cols):
        print(f"⚠️ {name} ({symbol}): 缺少必要列")
        return None

    df = df.reset_index()
    df.rename(columns={df.columns[0]: "date"}, inplace=True)
    df.columns = [col.lower() for col in df.columns]
    df = df[["date", "open", "close", "high", "low", "volume"]].copy()
    df["date"] = pd.to_datetime(df["date"])
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # === 关键：只转数值类型，保留 NaN，不转字符串！===
    for col in ["open", "close", "high", "low", "volume"]:
        df[col] = pd.to_numeric(df[col], errors='coerce')  # 非数字 → NaN

    # 按日期降序（与 Wind 一致）
    df.sort_values("date", ascending=False, inplace=True)

    if df.empty:
        return None

    print(f"✅ {name} ({symbol}): 抓取 {len(df)} 条原始数据")
    return df  # 注意：所有价格/volume 仍是 float，含 NaN

# ===================== 主流程 =====================
def _fetch_all(symbols: dict[str, str], end: str, params: dict) -> dict[str, pd.DataFrame]:
    """批量抓取多个标的"""
    return {
        sym: df for sym, df in (
            (sym, _fetch_clean(sym, start, end, params))
            for sym, start in symbols.items()
        ) if df is not None
    }

def generate_long_data():
    """生成全量长周期数据（保存到 long_path）"""
    w.start()
    if not w.isconnected():
        raise RuntimeError("❌ Wind 未连接")
    data = _fetch_all(CONFIG["symbols"], CONFIG["end_date"], CONFIG["wind_params"])
    for sym, df in data.items():
        _save_df(df, sym, CONFIG["long_path"])
    w.stop()

def update_short_data(new_end: str = NEW_DATE):
    """增量更新：基于 long_path 最新日期抓新数据，拼接后存入 short_path"""
    w.start()
    if not w.isconnected():
        raise RuntimeError("❌ Wind 未连接")

    # 确定每个标的的增量起始日
    starts = {sym: _read_latest_date(sym, CONFIG["long_path"]) for sym in CONFIG["symbols"]}
    new_data = _fetch_all(starts, new_end, CONFIG["wind_params"])

    for sym, short_df in new_data.items():
        long_fp = os.path.join(CONFIG["long_path"], f"{sym}.csv")
        if not os.path.exists(long_fp):
            continue
        long_df = pd.read_csv(long_fp, encoding="utf-8-sig")
        # 拼接 + 去重 + 降序
        combined = pd.concat([long_df, short_df], ignore_index=True)
        combined.drop_duplicates("date", keep="first", inplace=True)
        combined["date"] = pd.to_datetime(combined["date"])
        combined.sort_values("date", ascending=False, inplace=True)
        combined["date"] = combined["date"].dt.strftime("%Y-%m-%d")
        _save_df(combined, sym, CONFIG["short_path"])

    w.stop()

def generate_external_long_data(end_date: str = CONFIG["end_date"]):
    """生成海外指数全量数据（保存到 long_path）"""
    print("\n🌍 开始下载 Stooq 全量海外指数数据...\n")
    results = {}
    for symbol, start in EXTERNAL_SYMBOLS.items():
        name = symbol  # 可选：加个映射表美化日志
        df = _fetch_clean_stooq(name, symbol, start, end_date)
        if df is not None:
            results[symbol] = df

    for symbol, df in results.items():
        _save_df(df, symbol, CONFIG["long_path"])

def update_external_short_data(new_end: str = NEW_DATE):
    """增量更新海外指数数据（基于 long_path 最新日期，存入 short_path）"""
    print("\n🔄 增量更新 Stooq 海外指数数据...\n")
    
    # === 新增：读取 000300.SH 作为日期基准 ===
    target_fp = os.path.join(CONFIG["short_path"], "000300.SH.csv")
    if not os.path.exists(target_fp):
        raise FileNotFoundError(f"❌ 基准文件不存在: {target_fp}，请先生成沪深300日度数据")
    target_df = pd.read_csv(target_fp, usecols=["date"], encoding="utf-8-sig")
    target_dates = target_df["date"]

    starts = {sym: _read_external_latest_date(sym, CONFIG["long_path"]) for sym in EXTERNAL_SYMBOLS}
    new_data = {}
    for symbol, start in starts.items():
        name = symbol
        df = _fetch_clean_stooq(name, symbol, start, new_end)
        if df is not None:
            new_data[symbol] = df

    for symbol, short_df in new_data.items():
        long_fp = os.path.join(CONFIG["long_path"], f"{symbol}.csv")
        if not os.path.exists(long_fp):
            continue
        long_df = pd.read_csv(long_fp, encoding="utf-8-sig")
        
        # 合并新旧数据
        combined = pd.concat([long_df, short_df], ignore_index=True)
        combined.drop_duplicates("date", keep="first", inplace=True)
        combined["date"] = pd.to_datetime(combined["date"])
        combined.sort_values("date", ascending=False, inplace=True)
        combined["date"] = combined["date"].dt.strftime("%Y-%m-%d")

        # === 新增：步骤1 - 按 000300.SH 日期对齐 ===
        start_date = EXTERNAL_SYMBOLS[symbol]
        aligned_df = _align_to_target_dates(combined, symbol, target_dates, start_date)

        # === 新增：步骤2 - 重新执行空值处理（针对对齐后的数据）===
        aligned_df = _reprocess_nulls_for_aligned(aligned_df, new_end)

        # === 保存 ===
        _save_df(aligned_df, symbol, CONFIG["short_path"])

# ===================== 执行入口 =====================
if __name__ == "__main__":
    # Wind 数据
    # generate_long_data()
    # update_short_data()

    # Stooq 海外数据
    # generate_external_long_data(end_date=CONFIG["end_date"])
    update_external_short_data(new_end=NEW_DATE)


    