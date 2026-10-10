#!/usr/bin/env bash
# 启动 server + relay agent 的守护循环。用法: start.sh [--restart]
OPS="$(cd "$(dirname "$0")" && pwd)"
set -a; . "${BRIDGE_ENV:-$OPS/bridge.env}"; set +a
PORT="${BRIDGE_PORT:-8787}"
unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy
if [ "$1" = --restart ]; then pkill -f "$OPS/supervise.sh"; pkill -f "node src/server.mjs"; pkill -f "relay-agent.mjs"; sleep 2; fi
if pgrep -f "$OPS/supervise.sh" >/dev/null; then echo "supervisor already running"; else
  setsid nohup "$OPS/supervise.sh" >>"$OPS/supervise.log" 2>&1 < /dev/null & echo "supervisor started"; fi
for i in $(seq 1 30); do curl -fsS "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1 && { echo "local healthz ok"; break; }; sleep 1; done
