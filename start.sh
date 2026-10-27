#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

echo "Starting MyMusic backend v3.0..."
echo "Python: $(python --version)"
echo "yt-dlp: $(python -m yt_dlp --version 2>/dev/null || echo 'check import')"

# Main process
exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
