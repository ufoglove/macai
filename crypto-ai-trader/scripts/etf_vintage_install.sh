#!/bin/bash
# ETF 資金流 vintage 收集器排程（com.cryptoai.etf-vintage，每小時 :23）：只渲染並載入這一個 launchd 排程，不碰其他服務。
#   bash scripts/etf_vintage_install.sh              # 安裝／更新
#   bash scripts/etf_vintage_install.sh --status     # 看載入狀態
#   bash scripts/etf_vintage_install.sh --uninstall  # 卸下並把模板改名 .removed（start.sh 之後不會再自動載入）
# 模板在 launchd/（start.sh 也會自動同步排程模板）；網域走 scripts/launchd_domain.sh。說明：docs/ETF資金流vintage.md
set -u
cd "$(dirname "$0")/.."
ROOT="$(pwd -P)"
VENV_R="$(cd "$ROOT/.venv" && pwd -P)"
source scripts/launchd_domain.sh
S="etf-vintage"; L="com.cryptoai.$S"
tpl="launchd/$L.plist"; out="$HOME/Library/LaunchAgents/$L.plist"
mkdir -p "$HOME/Library/Logs/crypto-ai" "$HOME/Library/LaunchAgents"
case "${1:-}" in
  --status)
    d=$(launchd_where "$L") && echo "  ✓ ${S} 已載入（${d}）" || echo "  ✗ ${S} 未載入"; exit 0 ;;
  --uninstall)
    launchctl bootout "$LAUNCHD_DOMAIN/$L" 2>/dev/null
    rm -f "$out"
    [ -f "$tpl" ] && mv "$tpl" "$tpl.removed"
    echo "  · ${S} 已卸下"; exit 0 ;;
esac
[ -f "$tpl" ] || { echo "  ✗ 缺模板 ${tpl}" >&2; exit 1; }
sed "s#__ROOT__#$ROOT#g; s#__HOME__#$HOME#g; s#__VENV__#$VENV_R#g" "$tpl" > "$out.tmp" && plutil -lint "$out.tmp" >/dev/null \
  || { echo "  ✗ ${S} 模板渲染失敗" >&2; rm -f "$out.tmp"; exit 1; }
chmod 600 "$out.tmp"; mv "$out.tmp" "$out"
launchctl bootout "$LAUNCHD_DOMAIN/$L" 2>/dev/null
if launchctl bootstrap "$LAUNCHD_DOMAIN" "$out"; then echo "  ✓ ${S} 已載入（${LAUNCHD_DOMAIN}）"; else echo "  ✗ ${S} 載入失敗" >&2; exit 1; fi
