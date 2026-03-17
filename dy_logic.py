# -*- coding: utf-8 -*-
"""
DY 指标核心逻辑（由 Pine Script 移植）
蓝黄带 + MACD 打点 + 买卖信号与趋势信号
"""
import numpy as np
import pandas as pd
from typing import Tuple

# 默认参数（与 Pine 一致）
S, P, M = 12, 26, 9
BLUE_TOP_N, BLUE_BOT_N = 24, 23
YELLOW_TOP_N, YELLOW_BOT_N = 89, 90


def ema(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False).mean()


def crossover(a: pd.Series, b: pd.Series) -> pd.Series:
    return (a.shift(1) < b.shift(1)) & (a > b)


def crossunder(a: pd.Series, b: pd.Series) -> pd.Series:
    return (a.shift(1) > b.shift(1)) & (a < b)


def barssince_series(condition: pd.Series) -> pd.Series:
    """对每根 K 线计算：自上次 condition 为 True 以来的 bar 数（含当前）。"""
    out = np.zeros(len(condition), dtype=float)
    out[:] = np.nan
    last_true = -1
    for i in range(len(condition)):
        if condition.iloc[i]:
            last_true = i
        out[i] = i - last_true if last_true >= 0 else np.nan
    return pd.Series(out, index=condition.index)


def compute_dy_signals(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    s: int = S,
    p: int = P,
    m: int = M,
) -> pd.DataFrame:
    """
    输入 OHLC 的 high, low, close，输出包含以下列的 DataFrame（与 Pine 对齐）：
    buy, sell, up1, up2, up3, down1, down2, down3
    """
    n_bars = len(close)
    if n_bars < 2:
        return pd.DataFrame()

    # 蓝黄带
    blue_top = ema(high, BLUE_TOP_N)
    blue_bottom = ema(low, BLUE_BOT_N)
    yellow_top = ema(high, YELLOW_TOP_N)
    yellow_bottom = ema(low, YELLOW_BOT_N)

    # MACD
    fastMA = ema(close, s)
    slowMA = ema(close, p)
    DIFF = fastMA - slowMA
    DEA = ema(DIFF, m)
    MACD = (DIFF - DEA) * 2

    # 趋势信号（不依赖历史复杂状态）
    up1 = (close > blue_top) & (close.shift(1) < blue_top.shift(1))
    up2 = (close > yellow_top) & (close.shift(1) < yellow_top.shift(1))
    up3 = (blue_bottom > yellow_top) & (blue_bottom.shift(1) < yellow_top.shift(1))
    down1 = (close < blue_bottom) & (close.shift(1) > blue_bottom.shift(1))
    down2 = (close < yellow_bottom) & (close.shift(1) > yellow_bottom.shift(1))
    down3 = (blue_bottom < yellow_bottom) & (blue_bottom.shift(1) > yellow_bottom.shift(1))

    # MACD 死叉/金叉 barssince
    macd_death = (MACD.shift(1) >= 0) & (MACD < 0)
    macd_gold = (MACD.shift(1) <= 0) & (MACD > 0)
    N1 = barssince_series(macd_death)
    MM1 = barssince_series(macd_gold)

    valid_N1 = N1.fillna(1).clip(lower=0).astype(int)
    valid_MM1 = MM1.fillna(1).clip(lower=0).astype(int)

    def nz(x: pd.Series, default: float = 0) -> pd.Series:
        return x.fillna(default)

    def safe_lowest(close_ser: pd.Series, length: pd.Series) -> pd.Series:
        out = pd.Series(index=close_ser.index, dtype=float)
        for i in range(len(close_ser)):
            L = int(length.iloc[i]) + 1
            start = max(0, i - L + 1)
            out.iloc[i] = close_ser.iloc[start : i + 1].min()
        return out

    def safe_highest(close_ser: pd.Series, length: pd.Series) -> pd.Series:
        out = pd.Series(index=close_ser.index, dtype=float)
        for i in range(len(close_ser)):
            L = int(length.iloc[i]) + 1
            start = max(0, i - L + 1)
            out.iloc[i] = close_ser.iloc[start : i + 1].max()
        return out

    def var_shift(ser: pd.Series, lag: pd.Series, default: float = 0) -> pd.Series:
        out = pd.Series(index=ser.index, dtype=float)
        for i in range(len(ser)):
            k = int(lag.iloc[i]) + 1
            idx = i - k
            out.iloc[i] = ser.iloc[idx] if idx >= 0 else default
        return out

    CC1 = safe_lowest(close, valid_N1)
    CC2 = var_shift(CC1, valid_MM1)
    CC3 = var_shift(CC2, valid_MM1)

    DIFL1 = safe_lowest(DIFF, valid_N1)
    DIFL2 = var_shift(DIFL1, valid_MM1)
    DIFL3 = var_shift(DIFL2, valid_MM1)

    CH1 = safe_highest(close, valid_MM1)
    CH2 = var_shift(CH1, valid_N1)
    CH3 = var_shift(CH2, valid_N1)

    DIFH1 = safe_highest(DIFF, valid_MM1)
    DIFH2 = var_shift(DIFH1, valid_N1)
    DIFH3 = var_shift(DIFH2, valid_N1)

    AAA = (CC1 < CC2) & (DIFL1 > DIFL2) & (MACD.shift(1) < 0) & (DIFF < 0)
    BBB = (
        (CC1 < CC3)
        & (DIFL1 < DIFL2)
        & (DIFL1 > DIFL3)
        & (MACD.shift(1) < 0)
        & (DIFF < 0)
    )
    CCC = (AAA | BBB) & (DIFF < 0)

    AAA_int = AAA.astype(int)
    BBB_int = BBB.astype(int)
    CCC_int = CCC.astype(int)

    LLL = (nz(CCC_int.shift(1)) == 0) & CCC
    LLL_int = LLL.astype(int)
    XXX = (
        ((nz(AAA_int.shift(1)) != 0) & (DIFL1 <= DIFL2) & (DIFF < DEA))
        | ((nz(BBB_int.shift(1)) != 0) & (DIFL1 <= DIFL3) & (DIFF < DEA))
    )
    JJJ = (nz(CCC_int.shift(1)) != 0) & (
        np.abs(nz(DIFF.shift(1))) >= (np.abs(DIFF) * 1.01)
    )
    JJJ_int = JJJ.astype(int)
    BLBL = (nz(JJJ_int.shift(1)) != 0) & CCC & (
        nz(np.abs(DIFF.shift(1))) * 1.01 <= np.abs(DIFF)
    )
    DXDX = (nz(JJJ_int.shift(1)) == 0) & JJJ

    JJJ_float = JJJ.astype(float)

    def rolling_sum_24(ser: pd.Series) -> pd.Series:
        return ser.rolling(24, min_periods=1).sum()

    jjj_at_mm1_1 = var_shift(JJJ_int.astype(float), valid_MM1)
    jjj_at_mm1_0 = pd.Series(index=JJJ_int.index, dtype=float)
    for i in range(len(JJJ_int)):
        k = int(valid_MM1.iloc[i])
        idx = i - k
        jjj_at_mm1_0.iloc[i] = JJJ_int.iloc[idx] if idx >= 0 else 0

    DJGXX = (
        ((close < CC2) | (close < CC1))
        & ((jjj_at_mm1_1 != 0) | (jjj_at_mm1_0 != 0))
        & (nz(LLL_int.shift(1)) == 0)
        & (rolling_sum_24(JJJ_float) >= 1)
    )
    DJGXX_float = DJGXX.astype(float)
    sum_djgxx_2 = DJGXX_float.shift(1).rolling(2, min_periods=1).sum()
    DJXX = (sum_djgxx_2 < 1) & DJGXX
    DXX = (XXX | DJXX) & ~CCC

    ZJDBL = (CH1 > CH2) & (DIFH1 < DIFH2) & (MACD.shift(1) > 0) & (DIFF > 0)
    GXDBL = (
        (CH1 > CH3)
        & (DIFH1 > DIFH2)
        & (DIFH1 < DIFH3)
        & (MACD.shift(1) > 0)
        & (DIFF > 0)
    )
    DBBL = (ZJDBL | GXDBL) & (DIFF > 0)
    DBBL_int = DBBL.astype(int)
    ZJDBL_int = ZJDBL.astype(int)
    GXDBL_int = GXDBL.astype(int)
    DBL = (nz(DBBL_int.shift(1)) == 0) & (DBBL & (DIFF > DEA))
    DBL_int = DBL.astype(int)
    DBLXS = (
        ((nz(ZJDBL_int.shift(1)) != 0) & (DIFH1 >= DIFH2) & (DIFF > DEA))
        | ((nz(GXDBL_int.shift(1)) != 0) & (DIFH1 >= DIFH3) & (DIFF > DEA))
    )
    DBJG = (nz(DBBL_int.shift(1)) != 0) & (nz(DIFF.shift(1)) >= (DIFF * 1.01))
    DBJG_int = DBJG.astype(int)
    DBJG_float = DBJG.astype(float)
    DBJGXC = (nz(DBJG_int.shift(1)) == 0) & DBJG
    DBJGBL = (nz(DBJG_int.shift(1)) != 0) & DBBL & (nz(DIFF.shift(1)) * 1.01 <= DIFF)

    dbjg_at_n1_1 = var_shift(DBJG_int.astype(float), valid_N1)
    dbjg_at_n1_0 = pd.Series(index=DBJG_int.index, dtype=float)
    for i in range(len(DBJG_int)):
        k = int(valid_N1.iloc[i])
        idx = i - k
        dbjg_at_n1_0.iloc[i] = DBJG_int.iloc[idx] if idx >= 0 else 0

    ZZZZZ = (
        ((close > CH2) | (close > CH1))
        & ((dbjg_at_n1_1 != 0) | (dbjg_at_n1_0 != 0))
        & (nz(DBL_int.shift(1)) == 0)
        & (DBJG_float.rolling(23, min_periods=1).sum() >= 1)
    )
    ZZZZZ_float = ZZZZZ.astype(float)
    sum_zzzzz_2 = ZZZZZ_float.shift(1).rolling(2, min_periods=1).sum()
    YYYYY = (sum_zzzzz_2 < 1) & ZZZZZ
    WWWWW = (DBLXS | YYYYY) & ~DBBL

    buy = DXDX & crossover(DIFF, DEA)
    sell = DBJGXC & crossunder(DIFF, DEA)

    result = pd.DataFrame(
        {
            "buy": buy,
            "sell": sell,
            "up1": up1,
            "up2": up2,
            "up3": up3,
            "down1": down1,
            "down2": down2,
            "down3": down3,
        },
        index=close.index,
    )
    return result


def screener_row(high: pd.Series, low: pd.Series, close: pd.Series) -> dict:
    """
    对单标的计算最后一根 K 线的筛选结果，返回字典，用于表格一行。
    打点显示：有信号为 '🔴'，否则为空字符串。
    """
    df = compute_dy_signals(high, low, close)
    if df.empty:
        return {
            "Buy": "",
            "Sell": "",
            "UP1(c>bt)": "",
            "UP2(c>yt)": "",
            "UP3(bt>yt)": "",
            "DOWN1(c<bt)": "",
            "DOWN2(c<yt)": "",
            "DOWN3(bt<yt)": "",
        }
    last = df.iloc[-1]
    dot = "🔴"

    def to_dot(b: bool) -> str:
        return dot if b else ""

    return {
        "Buy": to_dot(last["buy"]),
        "Sell": to_dot(last["sell"]),
        "UP1(c>bt)": to_dot(last["up1"]),
        "UP2(c>yt)": to_dot(last["up2"]),
        "UP3(bt>yt)": to_dot(last["up3"]),
        "DOWN1(c<bt)": to_dot(last["down1"]),
        "DOWN2(c<yt)": to_dot(last["down2"]),
        "DOWN3(bt<yt)": to_dot(last["down3"]),
    }
