# 美股 DY 选股策略

由 TradingView Pine Script「DY with 打点」指标移植的选股框架：蓝黄带 + MACD 打点逻辑，对美股计算买卖与趋势信号并输出表格。

## 表格列说明

| 列名 | 含义 |
|------|------|
| Symbol | 标的代码 |
| Sector | 股票所在板块/ETF 分类 |
| Date | 最新交易日期 |
| Close | 最新收盘价 |
| Buy | 买入信号（DXDX 且 DIFF 上穿 DEA） |
| Sell | 卖出信号（DBJGXC 且 DIFF 下穿 DEA） |
| UP1(c>bt) | 收盘上穿蓝带上轨 |
| UP2(c>yt) | 收盘上穿黄带上轨 |
| UP3(bt>yt) | 蓝带下轨上穿黄带上轨 |
| DOWN1(c<bt) | 收盘下穿蓝带下轨 |
| DOWN2(c<yt) | 收盘下穿黄带下轨 |
| DOWN3(bt<yt) | 蓝带下轨下穿黄带下轨 |

有信号时显示 `🔴`，无信号为空。表格仅显示至少命中一个信号的股票，同时始终保留 `QQQ`、`SPY`、`UVXY`。

## 安装

```bash
cd dy_stock_screener
pip install -r requirements.txt
```

## 使用

- 默认使用内置美股列表 `QQQ`、`SPY`、`UVXY`，且只显示有任一信号的行：

```bash
python screener.py
```

- 指定标的、周期与 K 线间隔：

```bash
python screener.py --symbols AAPL MSFT GOOGL --period 1y --interval 1d
```

- 输出 CSV 与 HTML 表格：

```bash
python screener.py --csv result.csv --html result.html
```

- 仅保存不打印：

```bash
python screener.py --csv result.csv --no-print
```

## 目录结构

- `dy_logic.py`：DY 指标核心（蓝黄带、MACD、AAA/BBB/CCC、DXDX/DBJGXC 等），输出 buy/sell 与 up1–3、down1–3。
- `screener.py`：拉取行情、调用 `dy_logic`、补充 `Sector/Date/Close` 并输出打印/CSV/HTML。
- `sector_cache.json`：板块缓存，减少重复查询。
- `requirements.txt`：依赖（pandas, numpy, yfinance）。

数据来源为 yfinance，仅供学习与回测，实盘请自行核对数据与合规性。
