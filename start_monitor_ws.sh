#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3 -u -m binance_futures_monitor.monitor_ws --workers 3 --mode lob --control-file config/monitor_ws_control.json --onchain-targets-file config/onchain_targets.json --onchain-sector-file config/onchain_sectors.json
