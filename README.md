# oneoneo - monitor_ws 主进程 + Aster 子链路

本仓库包含：
- `binance_futures_monitor/monitor_ws.py` 主监控进程
- Aster 子链路（Aster WS / Aster REST / Aster 独立信号库配置）
- monitor_ws Web 面板（含 Aster 看板与 SlowAccum 分解页）

## 快速启动

### 1) Python 依赖
```bash
pip3 install -r requirements.txt
```

### 2) Web 依赖
```bash
npm install
```

### 3) 配置 webhook
编辑 `config/monitor_ws_control.json`：
- `discord_webhook_url`
- `aster_discord_webhook_url`

### 4) 启动监控
```bash
bash start_monitor_ws.sh
```

### 5) 启动 Web
```bash
bash start_web.sh
```

## 页面
- `/monitor-ws` 控制台
- `/monitor-ws-pushes` Binance 推送看板
- `/monitor-ws-pushes-aster` Aster 推送看板
- `/monitor-ws-slow-accum-aster` Aster SlowAccum 分解看板

## Aster 数据库快照
- 快照文件：`data/snapshots/monitor_ws_signals_aster_20260415.db.gz`
- 还原命令：
```bash
mkdir -p data/logs
gunzip -c data/snapshots/monitor_ws_signals_aster_20260415.db.gz > data/logs/monitor_ws_signals_aster.db
```
