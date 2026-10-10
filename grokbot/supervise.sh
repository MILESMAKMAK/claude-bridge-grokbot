#!/usr/bin/env bash
# 每 5 秒检查一次，server 或 relay agent 不在就拉起来（无 systemd 环境的自愈）
OPS="$(cd "$(dirname "$0")" && pwd)"
ENVF="${BRIDGE_ENV:-$OPS/bridge.env}"
set -a; . "$ENVF"; set +a
REPO="${BRIDGE_REPO:-$(dirname "$OPS")}"; DATA="${BRIDGE_DATA:-$HOME/bridge-srv}"; PORT="${BRIDGE_PORT:-8787}"
unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy
while true; do
  if ! pgrep -f "node src/server.mjs" >/dev/null; then
    echo "$(date -Is) starting server"
    ( cd "$REPO"
      NODE_ENV=production BRIDGE_EDITION=server BRIDGE_DATA_ROOT=$DATA HOME=$DATA/home PORT=$PORT BRIDGE_HOST=127.0.0.1 \
      SESSIONS_DIR=$DATA/dimensio/sessions MEMORY_DIR=$DATA/dimensio/memory KNOWLEDGE_DIR=$DATA/dimensio/knowledge \
      DIMENSIO_CONFIG_FILE=$DATA/dimensio/runtime-config.json DIMENSIO_QUICK_FILE=$DATA/dimensio/quick.json \
      PROJECTS_FILE=$DATA/dimensio/projects.json WORKSPACE_DIR=$DATA/dimensio/workspace HARNESS_ENV_FILE=$DATA/dimensio/.env \
      DIMENSIO_CUSTOM_PROVIDERS_DIR=$DATA/dimensio/custom-providers \
      setsid nohup node src/server.mjs >>"$OPS/bridge.log" 2>&1 < /dev/null & )
  fi
  if ! pgrep -f "relay-agent.mjs" >/dev/null; then
    echo "$(date -Is) starting relay agent"
    ( cd "$OPS"; RELAY_TARGET_PORT=$PORT setsid nohup node "$OPS/relay-agent.mjs" >>"$OPS/relay.log" 2>&1 < /dev/null & )
  fi
  sleep 5
done
