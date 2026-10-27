#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

# Download the Rust PO-token provider binary (single static binary, no deps).
BIN=./bgutil-pot
if [ ! -x "$BIN" ]; then
  echo "Downloading bgutil-pot..."
  for i in 1 2 3; do
    if curl -fL -o "$BIN" "https://github.com/jim60105/bgutil-ytdlp-pot-provider-rs/releases/latest/download/bgutil-pot-linux-x86_64"; then
      chmod +x "$BIN"
      break
    fi
    echo "download retry $i"; sleep 2
  done
fi

# Run the PO-token server in the background on localhost:4416 if binary is available
if [ -x "$BIN" ]; then
  ( "$BIN" server --host 127.0.0.1 --port 4416 > /tmp/bgutil-pot.log 2>&1 ) &
  for i in $(seq 1 30); do
    if curl -s http://127.0.0.1:4416/ping >/dev/null 2>&1; then
      echo "bgutil-pot server up on 127.0.0.1:4416"
      break
    fi
    sleep 1
  done
else
  echo "WARNING: bgutil-pot not executable; continuing without local POT provider"
fi

# Main process: the FastAPI app.
exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
