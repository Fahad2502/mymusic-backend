import ssl
ssl._create_default_https_context = ssl._create_unverified_context

import os
import re
import uuid
import time
import logging
import subprocess
import concurrent.futures
import httpx
import yt_dlp
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

# ── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("mymusic")

app = FastAPI(title="MyMusic Backend", version="2.1")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ---------------------------------------------------------------------------
# Caches
# ---------------------------------------------------------------------------
CACHE: dict[str, dict] = {}                             # stream uuid → info
_PREFETCH: dict[str, concurrent.futures.Future] = {}    # query → future
_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# Fast-search metadata cache: "query:limit" → (timestamp, results)
_SEARCH_CACHE: dict[str, tuple[float, list]] = {}
_SEARCH_CACHE_TTL = 300  # 5 minutes

# Audio extraction cache: video_id → (timestamp, song_dict)
_AUDIO_CACHE: dict[str, tuple[float, dict]] = {}
_AUDIO_CACHE_TTL = 900  # 15 minutes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _cookie_path() -> str | None:
    cf = os.environ.get("YT_COOKIES_FILE")
    if cf and os.path.exists(cf):
        return cf
    if os.path.exists("cookies.txt"):
        return os.path.abspath("cookies.txt")
    return None


def _is_pot_server_up() -> bool:
    """Check if the local bgutil-pot provider server is reachable on 127.0.0.1:4416."""
    try:
        r = httpx.get("http://127.0.0.1:4416/ping", timeout=0.8)
        return r.status_code == 200
    except Exception:
        return False


def _get_strategies() -> list[dict]:
    """
    Build yt-dlp extractor args strategies.
    TV client is placed FIRST because it does not trigger YouTube bot-guards
    on cloud/datacenter IP ranges (e.g. Render, AWS).
    """
    pot_active = _is_pot_server_up()

    strategies = [
        # Strategy 0: TV client — most resilient on datacenter IPs, no bot verification
        {
            "name": "tv",
            "args": {
                "youtube": {"player_client": ["tv"]}
            }
        },
        # Strategy 1: Android client
        {
            "name": "android",
            "args": {
                "youtube": {"player_client": ["android"]}
            }
        },
        # Strategy 2: iOS client
        {
            "name": "ios",
            "args": {
                "youtube": {"player_client": ["ios"]}
            }
        },
        # Strategy 3: Mobile Web client
        {
            "name": "mweb",
            "args": {
                "youtube": {"player_client": ["mweb"]}
            }
        },
    ]

    # If the PO-token server is active, add web_creator with PO-token provider
    if pot_active:
        strategies.insert(1, {
            "name": "pot_web_creator",
            "args": {
                "youtubepot-bgutilhttp": {"base_url": ["http://127.0.0.1:4416"]},
                "youtube": {"player_client": ["web_creator", "web"]}
            }
        })
    else:
        strategies.append({
            "name": "web_fallback",
            "args": {
                "youtube": {"player_client": ["web"]}
            }
        })

    return strategies


def _ydl_opts(strategy: dict) -> dict:
    opts = {
        "format": "bestaudio[ext=m4a]/bestaudio/best",
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "nocheckcertificate": True,
        "cachedir": "/tmp/yt-dlp-cache",
        "socket_timeout": 25,
        "retries": 3,
        "fragment_retries": 3,
        "extractor_args": strategy.get("args", {}),
    }

    # Add Node.js if present
    try:
        subprocess.run(["node", "--version"], capture_output=True, check=True)
        opts["js_runtimes"] = {"node": {}}
    except Exception:
        pass

    cp = _cookie_path()
    if cp:
        opts["cookiefile"] = cp

    return opts


def _pick_audio_url(song: dict) -> str | None:
    url = song.get("url")
    if url:
        return url
    fmts = song.get("formats") or []
    # Prefer audio-only formats
    audio_only = [f for f in fmts if f.get("vcodec") in (None, "none", "") and f.get("url") and f.get("acodec") != "none"]
    # Fallback: any format with audio
    any_audio = [f for f in fmts if f.get("url") and f.get("acodec") not in (None, "none", "")]
    # Last resort: any format with a URL
    any_fmt = [f for f in fmts if f.get("url")]
    chosen = (audio_only or any_audio or any_fmt or [None])[0]
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
    if len(CACHE) > 300:
        oldest = next(iter(CACHE))
        CACHE.pop(oldest, None)
    return {
        "title":     song.get("title"),
        "thumbnail": song.get("thumbnail"),
        "duration":  song.get("duration"),
        "audio_url": f"{base_url.rstrip('/')}/stream?id={req_id}",
    }


def _extract_by_id(video_id: str) -> dict | None:
    """Extract audio info for a specific YouTube video ID using resilient strategies."""
    cached = _AUDIO_CACHE.get(video_id)
    if cached and time.time() - cached[0] < _AUDIO_CACHE_TTL:
        log.info(f"Audio cache HIT for video_id={video_id}")
        return cached[1]

    url = f"https://www.youtube.com/watch?v={video_id}"
    strategies = _get_strategies()
    last_error = None

    for strat in strategies:
        try:
            log.info(f"Extracting {video_id} with strategy={strat['name']}")
            opts = _ydl_opts(strat)
            with yt_dlp.YoutubeDL(opts) as ydl:
                song = ydl.extract_info(url, download=False)
            if song and _pick_audio_url(song):
                log.info(f"Strategy {strat['name']} SUCCEEDED for {video_id}")
                _AUDIO_CACHE[video_id] = (time.time(), song)
                if len(_AUDIO_CACHE) > 80:
                    oldest = min(_AUDIO_CACHE, key=lambda k: _AUDIO_CACHE[k][0])
                    _AUDIO_CACHE.pop(oldest, None)
                return song
        except Exception as e:
            last_error = e
            log.warning(f"Strategy {strat['name']} failed for {video_id}: {e}")
            continue

    log.error(f"All extraction strategies failed for {video_id}: {last_error}")
    if last_error:
        raise last_error
    return None


def _extract_raw(search_term: str) -> dict | None:
    """Search and extract in one shot (for fallback / feeling lucky)."""
    strategies = _get_strategies()
    last_error = None

    for strat in strategies:
        try:
            log.info(f"Search-extract '{search_term}' with strategy={strat['name']}")
            opts = _ydl_opts(strat)
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(f"ytsearch1:{search_term}", download=False)
            entries = info.get("entries") or []
            if entries:
                song = entries[0]
                if _pick_audio_url(song):
                    log.info(f"Strategy {strat['name']} SUCCEEDED for '{search_term}'")
                    return song
        except Exception as e:
            last_error = e
            log.warning(f"Strategy {strat['name']} failed for '{search_term}': {e}")
            continue

    log.error(f"All strategies failed for '{search_term}': {last_error}")
    if last_error:
        raise last_error
    return None


def _search_metadata(query: str, limit: int = 8) -> list[dict]:
    """
    Fast metadata-only search using extract_flat.
    Does NOT download or extract audio URLs, which makes it 5-10x faster.
    """
    cache_key = f"{query}:{limit}"
    cached = _SEARCH_CACHE.get(cache_key)
    if cached and time.time() - cached[0] < _SEARCH_CACHE_TTL:
        return cached[1]

    opts = {
        "extract_flat": "in_playlist",
        "quiet": True,
        "no_warnings": True,
        "nocheckcertificate": True,
        "socket_timeout": 15,
        "retries": 2,
    }

    cp = _cookie_path()
    if cp:
        opts["cookiefile"] = cp

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)

        entries = info.get("entries") or []
        results = []
        for e in entries:
            vid = e.get("id") or e.get("url") or ""
            # Clean vid if it's full url
            if "/" in vid:
                vid = vid.split("/")[-1].split("=")[-1]
            fallback_thumb = f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg" if vid else ""
            results.append({
                "video_id": vid,
                "title": e.get("title", "Unknown"),
                "thumbnail": e.get("thumbnail") or fallback_thumb,
                "duration": e.get("duration"),
                "channel": e.get("channel") or e.get("uploader") or "",
            })

        if results:
            _SEARCH_CACHE[cache_key] = (time.time(), results)
            if len(_SEARCH_CACHE) > 150:
                oldest = min(_SEARCH_CACHE, key=lambda k: _SEARCH_CACHE[k][0])
                _SEARCH_CACHE.pop(oldest, None)

        return results
    except Exception as e:
        log.error(f"Flat search error for '{query}': {e}")
        return []


def _prefetch_bg(search_term: str):
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
# API Routes
# ---------------------------------------------------------------------------
@app.get("/")
def root():
    return {
        "status": "ok",
        "service": "mymusic-backend",
        "version": "2.1",
        "message": "Spotify-style music streaming API ready"
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/debug")
def debug():
    try:
        ver = yt_dlp.version.__version__
    except Exception:
        ver = "unknown"
    return {
        "status": "ok",
        "yt_dlp_version": ver,
        "pot_server_active": _is_pot_server_up(),
        "cookie_present": bool(_cookie_path()),
        "search_cache_entries": len(_SEARCH_CACHE),
        "audio_cache_entries": len(_AUDIO_CACHE),
        "stream_cache_entries": len(CACHE),
    }


# ── Fast metadata search ──────────────────────────────────────────────────
@app.get("/search_results")
def search_results(query: str, limit: int = 8):
    if not query or not query.strip():
        raise HTTPException(status_code=400, detail="query is required")

    q = query.strip()
    results = _search_metadata(q, min(limit, 12))
    if not results:
        # Fallback query
        results = _search_metadata(f"{q} music", min(limit, 12))
    return {"results": results}


# ── Play song by video_id ────────────────────────────────────────────────
@app.get("/play")
def play_song(video_id: str, request: Request):
    if not video_id or not video_id.strip():
        raise HTTPException(status_code=400, detail="video_id is required")

    vid = video_id.strip()
    try:
        song = _extract_by_id(vid)
    except Exception as e:
        log.error(f"/play failed for {vid}: {e}")
        raise HTTPException(status_code=502, detail=f"YouTube extraction failed: {e}")

    if not song:
        raise HTTPException(status_code=404, detail="Song could not be resolved from YouTube")

    try:
        result = _build_response(song, str(request.base_url))
    except ValueError as e:
        raise HTTPException(status_code=502, detail=f"No playable stream found: {e}")

    # Pre-fetch related song in background
    rel_q = _related_query(song.get("title", ""))
    if rel_q.strip():
        _PREFETCH[rel_q] = _EXECUTOR.submit(_prefetch_bg, rel_q)

    return result


# ── I'm Feeling Lucky / Direct search ───────────────────────────────────
@app.get("/search")
def search_song(query: str, request: Request, prefetch: str = ""):
    if not query or not query.strip():
        raise HTTPException(status_code=400, detail="query is required")

    q = query.strip()
    future = _PREFETCH.pop(q, None)
    if future is not None and future.done():
        song = future.result()
    else:
        if future:
            future.cancel()
        try:
            song = _extract_raw(f"{q} song")
        except Exception as e:
            log.error(f"/search extraction failed for '{q}': {e}")
            raise HTTPException(status_code=502, detail=f"Extraction failed: {e}")

    if not song:
        raise HTTPException(status_code=404, detail="Song not found")

    try:
        result = _build_response(song, str(request.base_url))
    except ValueError as e:
        raise HTTPException(status_code=502, detail=f"No playable stream: {e}")

    if prefetch and prefetch.strip():
        _PREFETCH[prefetch] = _EXECUTOR.submit(_prefetch_bg, f"{prefetch} song")

    rel_q = _related_query(song.get("title", q))
    _PREFETCH[rel_q] = _EXECUTOR.submit(_prefetch_bg, rel_q)

    return result


# ── Related Song ─────────────────────────────────────────────────────────
@app.get("/related")
def related_song(title: str, request: Request):
    rel_q = _related_query(title)
    future = _PREFETCH.pop(rel_q, None)
    song = None
    if future is not None:
        try:
            song = future.result(timeout=60)
        except Exception:
            song = None

    if not song:
        try:
            song = _extract_raw(rel_q)
        except Exception as e:
            log.error(f"/related failed for '{title}': {e}")
            raise HTTPException(status_code=502, detail=f"Extraction failed: {e}")

    if not song:
        raise HTTPException(status_code=404, detail="No related song found")

    try:
        result = _build_response(song, str(request.base_url))
    except ValueError as e:
        raise HTTPException(status_code=502, detail=f"No playable stream: {e}")

    next_q = _related_query(song.get("title", title))
    _PREFETCH[next_q] = _EXECUTOR.submit(_prefetch_bg, next_q)

    return result


# ── Audio Stream Proxy ───────────────────────────────────────────────────
@app.get("/stream")
def stream_song(id: str, request: Request):
    entry = CACHE.get(id)
    if not entry or not entry.get("url"):
        raise HTTPException(status_code=404, detail="Stream expired. Please search again.")

    url = entry["url"]
    headers = dict(entry.get("headers") or {})
    rng = request.headers.get("range")
    if rng:
        headers["range"] = rng

    # Clean headers that httpx handles automatically
    headers.pop("host", None)
    headers.pop("Host", None)

    client = httpx.Client(follow_redirects=True, timeout=httpx.Timeout(30.0, read=None))
    try:
        upstream = client.send(client.build_request("GET", url, headers=headers), stream=True)
    except Exception as e:
        client.close()
        raise HTTPException(status_code=502, detail=f"Could not connect to media stream: {e}")

    if upstream.status_code >= 400:
        code = upstream.status_code
        upstream.close()
        client.close()
        raise HTTPException(status_code=code, detail="Media stream rejected by provider")

    content_type = upstream.headers.get("content-type", "audio/mp4")

    def iter_bytes():
        try:
            for chunk in upstream.iter_bytes(chunk_size=65536):
                if chunk:
                    yield chunk
        finally:
            upstream.close()
            client.close()

    resp = StreamingResponse(iter_bytes(), media_type=content_type, status_code=upstream.status_code)
    for h in ("content-range", "content-length", "accept-ranges"):
        if h in upstream.headers:
            resp.headers[h] = upstream.headers[h]
    resp.headers["accept-ranges"] = "bytes"
    return resp
