# 副本使用指南（主进程 + Aster 子链路）

本指南用于你当前的副本仓库：
- 本地路径：`/Users/xiajunyao/Documents/New project/exports/oneoneo_main_aster_20260416_230307`
- 远端仓库：`https://github.com/xiamr73-sys/oneoneo.git`

## 1. 这个副本包含什么

该副本是独立可运行版本，包含：
- `binance_futures_monitor/monitor_ws.py` 主监控进程
- Aster 子链路（Aster WS + Aster REST + Aster 独立信号库配置）
- Web 面板（含 Binance 看板、Aster 看板、SlowAccum 分解看板）
- 启动脚本：
  - `start_monitor_ws.sh`
  - `start_web.sh`
- Aster 数据库快照：
  - `data/snapshots/monitor_ws_signals_aster_20260415.db.gz`

## 2. 首次使用（本地或 VPS）

进入目录：

```bash
cd /Users/xiajunyao/Documents/New\ project/exports/oneoneo_main_aster_20260416_230307
```

安装依赖：

```bash
pip3 install -r requirements.txt
npm install
```

## 3. 配置前必须做的事

编辑文件：

```bash
config/monitor_ws_control.json
```

至少确认以下字段：
- `discord_webhook_url`：主推送频道
- `aster_discord_webhook_url`：Aster 专属频道（如果你要单独推送）
- `aster_ws_enabled`：
  - `true` = 开启 Aster 子链路
  - `false` = 临时关闭 Aster 子链路

说明：仓库内 webhook 默认已清空，你需要填入你自己的值。

## 4. 启动顺序（推荐）

先启动监控进程，再启动网页：

```bash
bash start_monitor_ws.sh
```

新开一个终端窗口再启动网页：

```bash
bash start_web.sh
```

## 5. 如何验证是否正常运行

### 5.1 进程检查

```bash
ps aux | grep -E "binance_futures_monitor.monitor_ws|node src/index.js" | grep -v grep
```

### 5.2 运行状态 API

```bash
curl -s "http://127.0.0.1:8080/api/monitor-ws/runtime?lines=20"
```

### 5.3 看板页面

- 控制台：`http://127.0.0.1:8080/monitor-ws`
- Binance 推送看板：`http://127.0.0.1:8080/monitor-ws-pushes`
- Aster 推送看板：`http://127.0.0.1:8080/monitor-ws-pushes-aster`
- Aster SlowAccum 分解：`http://127.0.0.1:8080/monitor-ws-slow-accum-aster`

## 6. 恢复 Aster 历史数据库快照

如果需要把仓库里带的 Aster 快照还原到运行库：

```bash
mkdir -p data/logs
gunzip -c data/snapshots/monitor_ws_signals_aster_20260415.db.gz > data/logs/monitor_ws_signals_aster.db
```

## 7. 停止与重启

### 7.1 停止 monitor_ws

```bash
pkill -f "binance_futures_monitor.monitor_ws"
```

### 7.2 停止 Web

```bash
pkill -f "node src/index.js"
```

### 7.3 重启（标准流程）

```bash
pkill -f "binance_futures_monitor.monitor_ws"
pkill -f "node src/index.js"
bash start_monitor_ws.sh
bash start_web.sh
```

## 8. 如何切换 Aster 开关

编辑 `config/monitor_ws_control.json`：

- 开启 Aster：
  - `"aster_ws_enabled": true`
- 关闭 Aster：
  - `"aster_ws_enabled": false`

改完必须重启 monitor_ws 才会生效。

## 9. 常见问题排查

### 9.1 监控在跑，但 Aster 没有新数据

先看运行日志：

```bash
tail -n 100 data/logs/monitor_ws_console.log
```

如果出现连续：
- `[ws-aster] connecting ...`
- `[ws-aster] disconnected: ConnectionResetError()`

说明是 Aster 链路网络问题，不是主进程挂掉。

### 9.2 页面有数据但 Discord 没推送

重点检查：
- webhook 是否配置正确
- 分数阈值是否过高（如 `discord_min_score`）
- 信号级别是否被过滤（TIER4 可能只记录不推送）

### 9.3 配置改了但没生效

`monitor_ws_control.json` 改完要重启 monitor_ws。仅刷新网页不会让监控进程重读配置。

## 10. 与原项目并行运行的建议

如果你同时保留原项目和副本，建议：
- 使用不同 webhook
- 使用不同数据库文件路径
- 不要共用同一个 `data/logs/*.db`
- 先验证副本稳定后再替换生产实例

