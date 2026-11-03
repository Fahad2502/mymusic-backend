#!/usr/bin/env bash
set +e
cd "$(dirname "$0")"

# Start bgutil PO-token provider in background (bypasses YouTube bot-check on datacenter IPs)
# It runs as a local HTTP server that yt-dlp queries for PO tokens
BIN=./bgutil-pot
if [ -x "$BIN" ]; then
    "$BIN" server --host 127.0.0.1 --port 4416 >/tmp/bgutil.log 2>&1 &
    echo "bgutil-pot started (cached binary)"
else
    # Download bgutil binary on first boot
    (
        for i in 1 2 3; do
            if curl -fL -s --max-time 30 -o "$BIN" \
                "https://github.com/jim60105/bgutil-ytdlp-pot-provider-rs/releases/latest/download/bgutil-pot-linux-x86_64"; then
                chmod +x "$BIN" 2>/dev/null || true
                "$BIN" server --host 127.0.0.1 --port 4416 >/tmp/bgutil.log 2>&1 &
                echo "bgutil-pot downloaded and started"
                break
            fi
            sleep 3
        done
    ) &
fi

# Small delay to let bgutil start before uvicorn serves requests
sleep 2

# Main app — starts immediately so Render healthcheck passes
exec python -m uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
