#!/bin/bash
# DY策略每日扫描 - OpenClaw 定时任务
# 周二到周六 08:30 运行（对应美股周一到周五收盘后）

set -euo pipefail

PROJECT_DIR="/Users/apple/.openclaw/workspace/us-dy-screener"
DB_PATH="/Users/apple/.openclaw/workspace/uscd/us_stock_historical_full.db"
TELEGRAM_ENV="/Users/apple/.openclaw/workspace/uscd/.env_telegram"
RESULT_DIR="${PROJECT_DIR}/results/daily"
LOG_DIR="${PROJECT_DIR}/logs"

mkdir -p "${RESULT_DIR}" "${LOG_DIR}"
cd "${PROJECT_DIR}"

if [ -f "${TELEGRAM_ENV}" ]; then
    source "${TELEGRAM_ENV}"
fi

DATE="$(date +%Y%m%d)"
START_TIME="$(date +"%H:%M:%S")"
CSV_FILE="${RESULT_DIR}/dy_scan_${DATE}.csv"
HTML_FILE="${RESULT_DIR}/dy_scan_${DATE}.html"
SUMMARY_FILE="${RESULT_DIR}/dy_scan_${DATE}_summary.json"
MESSAGE_FILE="${RESULT_DIR}/dy_scan_${DATE}_message.txt"

echo "========================================"
echo "DY策略每日扫描 - ${DATE}"
echo "========================================"
echo "开始时间: ${START_TIME}"
echo "数据库: ${DB_PATH}"
echo ""

echo "【1/2】运行DY策略选股..."
python3 screener.py \
    --db-path "${DB_PATH}" \
    --csv "${CSV_FILE}" \
    --html "${HTML_FILE}" \
    --no-print

python3 <<EOF
from pathlib import Path
import json
import pandas as pd

csv_path = Path(r"${CSV_FILE}")
summary_path = Path(r"${SUMMARY_FILE}")
message_path = Path(r"${MESSAGE_FILE}")
signal_cols = ['Buy', 'Sell', 'UP1(c>bt)', 'UP2(c>yt)', 'UP3(bt>yt)', 'DOWN1(c<bt)', 'DOWN2(c<yt)', 'DOWN3(bt<yt)']

summary = {
    "status": "failed",
    "total_rows": 0,
    "active_rows": 0,
    "buy_count": 0,
    "sell_count": 0,
    "up1_count": 0,
    "up2_count": 0,
    "up3_count": 0,
    "down1_count": 0,
    "down2_count": 0,
    "down3_count": 0,
    "watchlist": "QQQ/SPY/UVXY",
}

message = "US DY 定时扫描失败，未生成结果文件。"

if csv_path.exists():
    df = pd.read_csv(csv_path)
    for col in signal_cols:
        if col not in df.columns:
            df[col] = ""

    def has_signal(row):
        return any(str(row[col]).strip() not in {"", "nan", "None"} for col in signal_cols)

    active = df[df.apply(has_signal, axis=1)].copy()
    counts = {col: int(active[col].fillna("").astype(str).str.strip().ne("").sum()) for col in signal_cols}
    watchlist = [s for s in ["QQQ", "SPY", "UVXY"] if s in df["Symbol"].astype(str).tolist()]

    summary = {
        "status": "ok",
        "total_rows": int(len(df)),
        "active_rows": int(len(active)),
        "buy_count": counts['Buy'],
        "sell_count": counts['Sell'],
        "up1_count": counts['UP1(c>bt)'],
        "up2_count": counts['UP2(c>yt)'],
        "up3_count": counts['UP3(bt>yt)'],
        "down1_count": counts['DOWN1(c<bt)'],
        "down2_count": counts['DOWN2(c<yt)'],
        "down3_count": counts['DOWN3(bt<yt)'],
        "watchlist": "/".join(watchlist) if watchlist else "QQQ/SPY/UVXY",
    }

    top_lines = []
    for _, row in active.head(8).iterrows():
        active_signals = [col for col in signal_cols if str(row[col]).strip() not in {"", "nan", "None"}]
        sector = str(row.get("Sector", "")).strip()
        sector_text = f" [{sector}]" if sector else ""
        top_lines.append(f"{row['Symbol']}{sector_text}: {', '.join(active_signals)}")

    top_text = "\n".join(top_lines) if top_lines else "无新增信号，仅保留 QQQ / SPY / UVXY 观察。"
    message = f"""📊 US DY 定时扫描完成
日期: ${DATE}
运行时间: ${START_TIME} - {__import__('datetime').datetime.now().strftime('%H:%M:%S')}

结果概览
- 表格总行数: {summary['total_rows']}
- 有信号股票: {summary['active_rows']}
- 保留观察: {summary['watchlist']}

信号统计
- Buy: {summary['buy_count']}
- Sell: {summary['sell_count']}
- UP1: {summary['up1_count']}
- UP2: {summary['up2_count']}
- UP3: {summary['up3_count']}
- DOWN1: {summary['down1_count']}
- DOWN2: {summary['down2_count']}
- DOWN3: {summary['down3_count']}

前排信号
{top_text}"""

summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
message_path.write_text(message, encoding="utf-8")
EOF

TOTAL_ROWS="$(python3 - <<EOF
import json
from pathlib import Path
data = json.loads(Path(r"${SUMMARY_FILE}").read_text(encoding="utf-8"))
print(data.get("total_rows", 0))
EOF
)"

ACTIVE_ROWS="$(python3 - <<EOF
import json
from pathlib import Path
data = json.loads(Path(r"${SUMMARY_FILE}").read_text(encoding="utf-8"))
print(data.get("active_rows", 0))
EOF
)"

END_TIME="$(date +"%H:%M:%S")"
echo "  扫描完成: ${TOTAL_ROWS}行, ${ACTIVE_ROWS}只有信号"

echo ""
echo "【2/2】发送Telegram通知..."

if [ -n "${TELEGRAM_BOT_TOKEN:-}" ] && [ -n "${TELEGRAM_CHAT_ID:-}" ]; then
    curl -sS -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
        -d "chat_id=${TELEGRAM_CHAT_ID}" \
        --data-urlencode "text@${MESSAGE_FILE}" > /dev/null

    if [ -f "${CSV_FILE}" ]; then
        curl -sS -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendDocument" \
            -F "chat_id=${TELEGRAM_CHAT_ID}" \
            -F "document=@${CSV_FILE}" \
            -F "caption=US DY 扫描结果 ${DATE}.csv" > /dev/null
    fi

    echo "✓ Telegram通知已发送"
else
    echo "⚠️ Telegram未配置"
fi

echo ""
echo "========================================"
echo "结束时间: ${END_TIME}"
echo "CSV: ${CSV_FILE}"
echo "HTML: ${HTML_FILE}"
echo "========================================"
