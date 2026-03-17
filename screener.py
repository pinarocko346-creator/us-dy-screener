# -*- coding: utf-8 -*-
"""
美股 DY 选股策略：拉取多标的、运行 DY 逻辑、输出表格
"""
import argparse
import json
import sqlite3
from pathlib import Path

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    yf = None

from dy_logic import screener_row

DEFAULT_SYMBOLS = ["QQQ", "SPY", "UVXY"]
ALWAYS_KEEP_SYMBOLS = {"QQQ", "SPY", "UVXY"}
SECTOR_CACHE_PATH = Path("/Users/apple/dy_stock_screener/sector_cache.json")

SIGNAL_COLUMNS = [
    "Buy",
    "Sell",
    "UP1(c>bt)",
    "UP2(c>yt)",
    "UP3(bt>yt)",
    "DOWN1(c<bt)",
    "DOWN2(c<yt)",
    "DOWN3(bt<yt)",
]

OUTPUT_COLUMNS = ["Symbol", "Sector", "Date", "Close", *SIGNAL_COLUMNS]


def empty_signal_row(
    symbol: str, last_date: str = "", last_close: float | str = "", sector: str = ""
) -> dict:
    return {
        "Symbol": symbol,
        "Sector": sector,
        "Date": last_date,
        "Close": last_close,
        "Buy": "",
        "Sell": "",
        "UP1(c>bt)": "",
        "UP2(c>yt)": "",
        "UP3(bt>yt)": "",
        "DOWN1(c<bt)": "",
        "DOWN2(c<yt)": "",
        "DOWN3(bt<yt)": "",
    }


def load_sector_cache() -> dict[str, str]:
    if not SECTOR_CACHE_PATH.exists():
        return {}
    try:
        return json.loads(SECTOR_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_sector_cache(cache: dict[str, str]) -> None:
    SECTOR_CACHE_PATH.write_text(
        json.dumps(dict(sorted(cache.items())), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def fetch_sector_for_symbol(symbol: str, cache: dict[str, str]) -> str:
    symbol = symbol.upper()
    if symbol in cache:
        return cache[symbol]

    if symbol in {"QQQ", "SPY", "UVXY"}:
        cache[symbol] = "ETF"
        return cache[symbol]

    if yf is None:
        cache[symbol] = ""
        return ""

    try:
        info = yf.Ticker(symbol).info
    except Exception:
        info = {}

    sector = (
        info.get("sector")
        or info.get("category")
        or ("ETF" if info.get("quoteType") == "ETF" else "")
        or info.get("industry")
        or ""
    )
    cache[symbol] = sector
    return sector


def enrich_sector(table: pd.DataFrame) -> pd.DataFrame:
    if table.empty:
        return table

    cache = load_sector_cache()
    table = table.copy()
    table["Sector"] = table["Symbol"].astype(str).str.upper().map(
        lambda symbol: fetch_sector_for_symbol(symbol, cache)
    )
    save_sector_cache(cache)
    return table


def finalize_table(table: pd.DataFrame) -> pd.DataFrame:
    if table.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    def active_signals(row: pd.Series) -> list[str]:
        return [col for col in SIGNAL_COLUMNS if str(row[col]).strip() not in {"", "-"}]

    table = table.copy()
    table["_signal_group"] = table.apply(
        lambda row: " | ".join(active_signals(row)),
        axis=1,
    )

    mask = (table["_signal_group"] != "") | (
        table["Symbol"].astype(str).str.upper().isin(ALWAYS_KEEP_SYMBOLS)
    )
    table = table.loc[mask].reset_index(drop=True)

    table = enrich_sector(table)

    active_mask = table["_signal_group"] != ""
    table["_group_order"] = (~active_mask).astype(int)
    table = table.sort_values(
        by=["_group_order", "_signal_group", "Sector", "Symbol"],
        ascending=[True, True, True, True],
    ).drop(columns=["_group_order", "_signal_group"])

    return table[OUTPUT_COLUMNS].reset_index(drop=True)


def fetch_ohlc(symbol: str, period: str = "6mo", interval: str = "1d") -> pd.DataFrame:
    """拉取单标的 OHLC，列名小写 high/low/close。"""
    if yf is None:
        raise ImportError("请安装 yfinance: pip install yfinance")
    ticker = yf.Ticker(symbol)
    df = ticker.history(period=period, interval=interval)
    if df.empty or len(df) < 100:
        return pd.DataFrame()
    df = df.rename(columns={c: c.lower() for c in df.columns})
    return df[["high", "low", "close"]].copy()


def run_screener(
    symbols: list[str] | None = None,
    period: str = "6mo",
    interval: str = "1d",
) -> pd.DataFrame:
    """对多标的运行 DY 筛选，只返回至少出现一个信号的标的。"""
    symbols = symbols or DEFAULT_SYMBOLS
    rows = []
    for sym in symbols:
        sym = (sym or "").strip()
        if not sym:
            continue
        try:
            ohlc = fetch_ohlc(sym, period=period, interval=interval)
            if ohlc.empty:
                row = empty_signal_row(sym)
            else:
                sig = screener_row(ohlc["high"], ohlc["low"], ohlc["close"])
                last = ohlc.iloc[-1]
                row = {
                    "Symbol": sym,
                    "Sector": "",
                    "Date": str(last.name.date()) if hasattr(last.name, "date") else "",
                    "Close": round(float(last["close"]), 4),
                    **sig,
                }
        except Exception as e:
            row = {
                "Symbol": sym,
                "Sector": "",
                "Date": "",
                "Close": "",
                "Buy": "-",
                "Sell": "-",
                "UP1(c>bt)": "-",
                "UP2(c>yt)": "-",
                "UP3(bt>yt)": "-",
                "DOWN1(c<bt)": "-",
                "DOWN2(c<yt)": "-",
                "DOWN3(bt<yt)": f"err:{e}",
            }
        rows.append(row)

    return finalize_table(pd.DataFrame(rows))


def run_screener_from_db(
    db_path: str,
    symbols: list[str] | None = None,
    min_bars: int = 100,
) -> pd.DataFrame:
    """从本地 SQLite 历史库运行 DY 筛选，只返回至少出现一个信号的标的。"""
    conn = sqlite3.connect(db_path)
    try:
        base_sql = (
            "SELECT code AS symbol, date, high, low, close "
            "FROM kline_data "
        )
        params: list[str] = []
        if symbols:
            placeholders = ",".join("?" for _ in symbols)
            sql = f"{base_sql} WHERE code IN ({placeholders}) ORDER BY code, date"
            params = symbols
        else:
            sql = f"{base_sql} ORDER BY code, date"

        df = pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()

    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    rows = []
    for symbol, group in df.groupby("symbol", sort=True):
        group = group.reset_index(drop=True)
        if len(group) < min_bars:
            continue
        try:
            sig = screener_row(group["high"], group["low"], group["close"])
            last = group.iloc[-1]
            rows.append(
                {
                    "Symbol": symbol,
                    "Sector": "",
                    "Date": str(last["date"]),
                    "Close": round(float(last["close"]), 4),
                    **sig,
                }
            )
        except Exception as e:
            rows.append(
                {
                    "Symbol": symbol,
                    "Sector": "",
                    "Date": "",
                    "Close": "",
                    "Buy": "-",
                    "Sell": "-",
                    "UP1(c>bt)": "-",
                    "UP2(c>yt)": "-",
                    "UP3(bt>yt)": "-",
                    "DOWN1(c<bt)": "-",
                    "DOWN2(c<yt)": "-",
                    "DOWN3(bt<yt)": f"err:{e}",
                }
            )

    return finalize_table(pd.DataFrame(rows))


def main():
    parser = argparse.ArgumentParser(description="美股 DY 选股策略 - 输出表格")
    parser.add_argument("--symbols", nargs="*", default=None, help="标的代码，默认使用内置列表")
    parser.add_argument("--period", default="6mo", help="拉取周期，如 6mo, 1y")
    parser.add_argument("--interval", default="1d", help="K 线周期，如 1d, 1h")
    parser.add_argument("--db-path", default="", help="本地 SQLite 数据库路径，表为 kline_data(code,date,high,low,close)")
    parser.add_argument("--csv", type=str, default="", help="输出 CSV 路径")
    parser.add_argument("--html", type=str, default="", help="输出 HTML 表格路径")
    parser.add_argument("--no-print", action="store_true", help="不打印到终端")
    args = parser.parse_args()

    symbols = args.symbols if args.symbols else DEFAULT_SYMBOLS
    if args.db_path:
        table = run_screener_from_db(db_path=args.db_path, symbols=args.symbols)
    else:
        table = run_screener(symbols=symbols, period=args.period, interval=args.interval)

    if not args.no_print:
        pd.set_option("display.unicode.east_asian_width", True)
        pd.set_option("display.max_columns", None)
        print(table.to_string(index=False))

    if args.csv:
        out = Path(args.csv)
        out.parent.mkdir(parents=True, exist_ok=True)
        table.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"已保存 CSV: {out}")

    if args.html:
        out = Path(args.html)
        out.parent.mkdir(parents=True, exist_ok=True)
        html = table.to_html(index=False, classes="dy-table", border=1)
        out.write_text(
            f"<!DOCTYPE html><html><head><meta charset='utf-8'/><title>DY 选股</title>"
            f"<style>table.dy-table {{ border-collapse: collapse; }} .dy-table th, .dy-table td {{ border: 1px solid #333; padding: 6px 10px; }} .dy-table th {{ background: #001154; color: #fff; }}</style>"
            f"</head><body>{html}</body></html>",
            encoding="utf-8",
        )
        print(f"已保存 HTML: {out}")

    return table


if __name__ == "__main__":
    main()
