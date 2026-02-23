import yfinance as yf
import pandas as pd

# 设置日经225指数代码
ticker = "^N225"

print(f"正在下载 {ticker} 最近20年的数据...")

# 下载20年日线数据（yfinance 的 period="20y" 可能不精确，建议用 start/end）
# 当前时间是 2026-02-21，所以从 2006-01-01 开始
data = yf.download(ticker, start="2006-01-04", end="2026-02-20", threads=False)

# 检查是否成功获取数据
if data.empty:
    print("❌ 未能获取数据，请检查网络或稍后再试。")
else:
    # 保留所需列（已默认包含 OHLCV）
    required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    data = data[required_columns]

    # 保存为 CSV
    output_file = "nikkei225_20y.csv"
    data.to_csv(output_file)

    print(f"✅ 成功下载 {len(data)} 个交易日的数据")
    print(f"📅 时间范围: {data.index.min().date()} 至 {data.index.max().date()}")
    print(f"💾 已保存至: {output_file}")

    # 显示最后5行预览
    print("\n📊 最后5行数据预览:")
    print(data.tail())