import pandas_datareader.data as web
import pandas as pd
from datetime import datetime
import os  # 用于文件和目录操作

# ================= 配置区域 =================

# 1. 设置保存的文件夹路径
# 你可以修改为绝对路径，例如: save_dir = r"D:\MyStockData"
# 默认在当前脚本所在目录下创建一个名为 "Global_Market_Data" 的文件夹
save_dir = r"/Users/zjy/python/ETF/ETF跟踪指数量价数据-海外数据"

# 2. 设置时间范围 (过去20年)
start = datetime(2026, 1, 4)
end = datetime.now()

# 3. 定义目标列表
targets = {
    "道琼斯工业指数":"^DJI",
    "S&P 500 指数": "^SPX",
    "日经 225 指数": "^NKX",
    "日经 225 ETF (野村)": "1321.JP",
    "德国 DAX 指数": "^DAX",
    "德国 DAX ETF (iShares)": "EXS1.DE"
}

# ================= 执行逻辑 =================

# 检查文件夹是否存在，不存在则创建
if not os.path.exists(save_dir):
    os.makedirs(save_dir)
    print(f"📂 创建新文件夹: {save_dir}")
else:
    print(f"📂 使用现有文件夹: {save_dir}")

print(f"🚀 开始下载数据 (区间: {start.date()} 到 {end.date()})...\n")

for name, ticker in targets.items():
    try:
        # 下载数据
        df = web.DataReader(ticker, 'stooq', start, end)
        
        # 反转顺序
        # df = df.iloc[::-1]
        
        # 填充 NaN 成交量
        # df['Volume'] = df['Volume'].fillna(0)
        
        # --- 关键修改：拼接完整的文件路径 ---
        # os.path.join 会自动处理 Windows/Mac 的路径分隔符
        filename = f"{ticker}.csv"
        full_path = os.path.join(save_dir, filename)
        
        # 保存文件
        df.to_csv(full_path)
        
        # 打印信息
        start_date = df.index[0].strftime('%Y-%m-%d')
        end_date = df.index[-1].strftime('%Y-%m-%d')
        
        print(f"✅ [已保存] {name}")
        print(f"   文件路径: {full_path}")
        print(f"   时间跨度: {start_date} 至 {end_date} ({len(df)} 条)")
        print("-" * 50)
        
    except Exception as e:
        print(f"❌ [失败] {name}: {e}")
        print("-" * 50)

print(f"\n🎉 所有任务完成。请查看文件夹: {os.path.abspath(save_dir)}")


