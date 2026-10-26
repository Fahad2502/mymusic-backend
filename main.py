import ssl
ssl._create_default_https_context = ssl._create_unverified_context

import os
import re
import uuid
import time
import subprocess
import concurrent.futures
import httpx
import yt_dlp
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

app = FastAPI(title="MyMusic Backend")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ---------------------------------------------------------------------------
# Caches
# ---------------------------------------------------------------------------
CACHE: dict[str, dict] = {}                             # stream uuid → info
_PREFETCH: dict[str, concurrent.futures.Future] = {}    # query → future
_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# Fast-search metadata cache: "query:limit" → (timestamp, results)
_SEARCH_CACHE: dict[str, tuple[float, list]] = {}
_SEARCH_CACHE_TTL = 300  # seconds

# Kill any leftover bgutil-pot from a previous deploy
try:
    subprocess.run(["pkill", "-f", "bgutil-pot"], capture_output=True)
    if os.path.exists("/tmp/bgutil-pot"):
        os.remove("/tmp/bgutil-pot")
except Exception:
    pass


# ---------------------------------------------------------------------------
# yt-dlp helpers
# ---------------------------------------------------------------------------
def _cookie_path() -> str | None:
    cf = os.environ.get("YT_COOKIES_FILE")
    if cf and os.path.exists(cf):
        return cf
    if os.path.exists("cookies.txt"):
        return os.path.abspath("cookies.txt")
    return None


def _ydl_opts() -> dict:
    opts = {
        "format": "bestaudio/best",
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "nocheckcertificate": True,
        "js_runtimes": {"node": {}},
        "cachedir": "/tmp/yt-dlp-cache",
        "extractor_args": {"youtube": ["player_client=web"]},
    }
    cp = _cookie_path()
    if cp:
        opts["cookiefile"] = cp
    return opts


def _extract_raw(search_term: str) -> dict | None:
    """Full extraction for a search term — returns first result with audio URLs."""
    with yt_dlp.YoutubeDL(_ydl_opts()) as ydl:
        info = ydl.extract_info(f"ytsearch1:{search_term}", download=False)
    entries = info.get("entries") or []
    return entries[0] if entries else None


def _extract_by_id(video_id: str) -> dict | None:
    """Full extraction for a specific YouTube video ID."""
    with yt_dlp.YoutubeDL(_ydl_opts()) as ydl:
        return ydl.extract_info(
            f"https://www.youtube.com/watch?v={video_id}", download=False
        )


def _search_metadata(query: str, limit: int = 6) -> list[dict]:
    """
    Fast metadata-only search using extract_flat.
    Returns titles, thumbnails, durations, and video IDs — NO audio extraction.
    This is 5-10× faster than full extraction.
    """
    cache_key = f"{query}:{limit}"
    cached = _SEARCH_CACHE.get(cache_key)
    if cached and time.time() - cached[0] < _SEARCH_CACHE_TTL:
        return cached[1]

    opts = _ydl_opts()
    opts["extract_flat"] = "in_playlist"
    opts.pop("format", None)  # not needed for flat extraction

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
    except Exception:
        return []

    entries = info.get("entries") or []
    results = []
    for e in entries:
        vid = e.get("id", "")
        # Construct thumbnail from video ID (reliable fallback)
        fallback_thumb = f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg" if vid else ""
        results.append({
            "video_id": vid,
            "title": e.get("title", "Unknown"),
            "thumbnail": e.get("thumbnail") or fallback_thumb,
            "duration": e.get("duration"),
            "channel": e.get("channel") or e.get("uploader") or "",
        })

    # Store in cache
    _SEARCH_CACHE[cache_key] = (time.time(), results)
    if len(_SEARCH_CACHE) > 100:
        oldest = min(_SEARCH_CACHE, key=lambda k: _SEARCH_CACHE[k][0])
        _SEARCH_CACHE.pop(oldest, None)

    return results


def _pick_audio_url(song: dict) -> str | None:
    url = song.get("url")
    if url:
        return url
    fmts = song.get("formats") or []
    audio_only = [f for f in fmts if f.get("vcodec") in (None, "none", "") and f.get("url")]
    any_fmt   = [f for f in fmts if f.get("url")]
    chosen = (audio_only or any_fmt or [None])[0]
    return chosen["url"] if chosen else None


def _build_response(song: dict, base_url: str) -> dict:
    audio_url = _pick_audio_url(song)
    if not audio_url:
        raise ValueError("No playable URL in extracted info")
    req_id = str(uuid.uuid4())
    CACHE[req_id] = {
        "url": audio_url,
        "headers": song.get("http_headers") or {},
        "title": song.get("title"),
        "thumbnail": song.get("thumbnail"),
        "duration": song.get("duration"),
    }
    if len(CACHE) > 200:
        oldest = next(iter(CACHE))
        CACHE.pop(oldest, None)
    return {
        "title":     song.get("title"),
        "thumbnail": song.get("thumbnail"),
        "duration":  song.get("duration"),
        "audio_url": f"{base_url.rstrip('/')}/stream?id={req_id}",
    }


def _prefetch_bg(search_term: str):
    """Run in thread pool — pre-extract so the next call is instant."""
    try:
        return _extract_raw(search_term)
    except Exception:
        return None


def _related_query(title: str) -> str:
    clean = re.sub(
        r'\(.*?\)|\[.*?\]|official|video|audio|lyrics|hd|4k|remaster|ft\.?|feat\.?',
        '', title, flags=re.I
    ).strip()
    words = clean.split()[:4]
    return " ".join(words) + " similar song"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/")
def root():
    return {"status": "ok", "service": "mymusic-backend"}


@app.get("/health")
def health():
    return {"status": "ok"}


# ── NEW: Fast metadata search (no audio extraction) ──────────────────────
@app.get("/search_results")
def search_results(query: str, limit: int = 6):
    """
    Returns multiple search results with metadata only — no audio extraction.
    This is dramatically faster than /search because it skips the expensive
    per-video extraction step.
    """
    if not query or not query.strip():
        raise HTTPException(status_code=400, detail="query is required")

    results = _search_metadata(query.strip(), min(limit, 10))
    return {"results": results}


# ── NEW: Play a specific video by ID ─────────────────────────────────────
@app.get("/play")
def play_song(video_id: str, request: Request):
    """Extract the audio stream URL for a specific YouTube video ID."""
    if not video_id or not video_id.strip():
        raise HTTPException(status_code=400, detail="video_id is required")

    try:
        song = _extract_by_id(video_id.strip())
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Extraction failed: {e}")

    if not song:
        raise HTTPException(status_code=404, detail="Song not found")

    result = _build_response(song, str(request.base_url))

    # Pre-fetch a related song in the background
    rel_q = _related_query(song.get("title", ""))
    if rel_q.strip():
        _PREFETCH[rel_q] = _EXECUTOR.submit(_prefetch_bg, rel_q)

    return result


# ── Existing: I'm Feeling Lucky (search + extract in one shot) ───────────
@app.get("/search")
def search_song(query: str, request: Request, prefetch: str = ""):
    if not query or not query.strip():
        raise HTTPException(status_code=400, detail="query is required")

    future = _PREFETCH.pop(query, None)
    if future is not None and future.done():
        song = future.result()
    else:
        if future:
            future.cancel()
        try:
            song = _extract_raw(f"{query} song")
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Extraction failed: {e}")

    if not song:
        raise HTTPException(status_code=404, detail="Song not found")

    result = _build_response(song, str(request.base_url))

    if prefetch and prefetch.strip():
        _PREFETCH[prefetch] = _EXECUTOR.submit(_prefetch_bg, f"{prefetch} song")

    rel_q = _related_query(song.get("title", query))
    _PREFETCH[rel_q] = _EXECUTOR.submit(_prefetch_bg, rel_q)

    return result


# ── Existing: Related song ───────────────────────────────────────────────
@app.get("/related")
def related_song(title: str, request: Request):
    rel_q = _related_query(title)

    future = _PREFETCH.pop(rel_q, None)
    if future is not None:
        try:
            song = future.result(timeout=90)
        except Exception:
            song = None
    else:
        song = None

    if not song:
        try:
            song = _extract_raw(rel_q)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Extraction failed: {e}")

    if not song:
        raise HTTPException(status_code=404, detail="No related song found")

    result = _build_response(song, str(request.base_url))

    next_q = _related_query(song.get("title", title))
    _PREFETCH[next_q] = _EXECUTOR.submit(_prefetch_bg, next_q)

    return result


# ── Existing: Audio stream proxy ─────────────────────────────────────────
@app.get("/stream")
def stream_song(id: str, request: Request):
    entry = CACHE.get(id)
    if not entry or not entry.get("url"):
        raise HTTPException(status_code=404, detail="Stream expired. Search again.")

    url     = entry["url"]
    headers = dict(entry.get("headers") or {})
    rng     = request.headers.get("range")
    if rng:
        headers["range"] = rng

    client = httpx.Client(follow_redirects=True, timeout=httpx.Timeout(30.0, read=None))
    try:
        upstream = client.send(client.build_request("GET", url, headers=headers), stream=True)
    except Exception as e:
        client.close()
        raise HTTPException(status_code=502, detail=f"Could not reach YouTube: {e}")

    if upstream.status_code >= 400:
        code = upstream.status_code
        upstream.close()
        client.close()
        raise HTTPException(status_code=code, detail="YouTube rejected the stream")

    content_type = upstream.headers.get("content-type", "audio/mp4")

    def iter_bytes():
        try:
            for chunk in upstream.iter_bytes():
                if chunk:
                    yield chunk
        finally:
            upstream.close()
            client.close()

    resp = StreamingResponse(iter_bytes(), media_type=content_type, status_code=upstream.status_code)
    for h in ("content-range", "content-length"):
        if h in upstream.headers:
            resp.headers[h] = upstream.headers[h]
    resp.headers["accept-ranges"] = "bytes"
    return resp
