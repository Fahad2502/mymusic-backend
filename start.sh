#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

# Optional background startup of bgutil POT provider for yt-dlp fallback
BIN=./bgutil-pot
if [ -x "$BIN" ]; then
  ( "$BIN" server --host 127.0.0.1 --port 4416 > /tmp/bgutil-pot.log 2>&1 ) &
elif [ ! -x "$BIN" ]; then
  (
    for i in 1 2; do
      if curl -fL -s -o "$BIN" "https://github.com/jim60105/bgutil-ytdlp-pot-provider-rs/releases/latest/download/bgutil-pot-linux-x86_64"; then
        chmod +x "$BIN"
        "$BIN" server --host 127.0.0.1 --port 4416 > /tmp/bgutil-pot.log 2>&1 &
        break
      fi
      sleep 2
    done
  ) &
fi

# Main process: the FastAPI app starts immediately so Render healthcheck passes
exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
