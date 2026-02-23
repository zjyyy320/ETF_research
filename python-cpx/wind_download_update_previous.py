from WindPy import w
import pandas as pd
import os
import numpy as np
import warnings

pd.options.mode.chained_assignment = None
warnings.filterwarnings('ignore', category=FutureWarning)



# ===================== 配置 =====================
CONFIG = {
    "end_date": "2026-02-05",
    "long_path": r"/Users/zjy/python/ETF/ETF跟踪指数量价数据-非日度更新/",
    "short_path": r"/Users/zjy/python/ETF/ETF跟踪指数量价数据-日度更新/",
    "wind_params": {
        "Days": "Trading",        # 仅抓取交易日数据
        "Fill": "Previous",       # 空值用前值（代码层不额外处理）
        "Order": "D",             # 数据按日期降序返回
        "Period": "D",            # 日频数据
        "TradingCalendar": "SSE", # 以上交所交易日历为准
        "Currency": "Original",   # 原始货币单位
        "PriceAdj": "U"           # 价格不复权Calendar": "SSE", "Currency": "Original", "PriceAdj": "U"
    },
    "symbols": {
    "980092.CNI": "2013-01-02",    # 自由现金流指数
    "h30269.CSI": "2018-05-11",    # 红利低波指数
    "000905.SH": "2005-01-04",     # 中证500指数
    "932000.CSI": "2014-01-02",    # 中证2000指数
    "000688.SH": "2020-01-02",     # 科创50指数
    "000300.SH": "2005-01-04",     # 沪深300指数
    "HSHYLV.HI": "2017-05-22",     # 恒生港股通高股息低波动指数
    "HSBIO.HI": "2019-12-16",      # 恒生生物科技指数
    "HSTECH.HI": "2020-07-27",     # 恒生科技指数
    "930709.CSI": "2020-02-24",    # 香港证券指数
    "DJI.GI": "1997-11-07",        # 道琼斯工业平均指数
    "000201.CZC": "2010-03-02",    # 易盛能化A指数
    "NH0700.NHF": "2018-06-29",    # 南华有色金属指数
    "NH0015.NHF": "2004-06-01",    # 南华豆粕指数
    "AU.SHF": "2008-01-09",        # SHFE黄金期货
    "511380.SH": "2020-04-07",     # 可转债ETF（博时）
    "511020.SH": "2019-02-22",     # 国债ETF（5至10年）
    "511260.SH": "2017-08-24",     # 十年国债ETF
    "511220.SH": "2014-12-16",     # 城投债ETF（海富通）
    "159972.SZ": "2019-11-08",     # 5年地债ETF
    "159816.SZ": "2020-09-04"      # 0-4年地债ETF
    }
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
    """从已有CSV读取最新日期（用于增量起点）"""
    fp = os.path.join(path, f"{symbol}.csv")
    if not os.path.exists(fp):
        return CONFIG["symbols"][symbol]
    try:
        date_str = pd.read_csv(fp, usecols=["date"], nrows=1).iloc[0]["date"]
        return pd.to_datetime(date_str).strftime("%Y-%m-%d")
    except Exception:
        return CONFIG["symbols"][symbol]

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

    # 删除含空值的行
    mask = df[["open","close","high","low","volume"]].isnull().any(axis=1)
    for d in df[mask]["date"].dt.strftime("%Y-%m-%d"):
        print(f"⚠️ {symbol}: {d} 含空值，已剔除")
    df = df[~mask].copy()
    if df.empty:
        return None

    # 格式标准化：价格4位小数，成交量整数
    for col in ["open","close","high","low"]:
        df[col] = df[col].astype(float).apply(lambda x: f"{x:.4f}")
    df["volume"] = df["volume"].astype(float).round().astype(int).astype(str)
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    print(f"✅ {symbol}: 抓取 {len(df)} 条有效数据")
    return df

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

def update_short_data(new_end: str = "2026-02-13"):
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

# ===================== 执行入口 =====================
if __name__ == "__main__":
    # generate_long_data()      # 生成基准长周期数据
    update_short_data()       # 增量更新并保存日度文件