"""
MyMusic Backend v4.0 — INSTANT audio extraction
Priority order (fastest first):
  1. InnerTube API direct call (~0.3–0.8s) — bypasses yt-dlp overhead entirely
  2. Invidious public API (~1–3s)
  3. yt-dlp tv_embedded (last resort, ~5–15s)
"""
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

import os
import re
import uuid
import time
import json
import logging
import httpx
import yt_dlp
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, RedirectResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("mymusic")

app = FastAPI(title="MyMusic Backend", version="4.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ---------------------------------------------------------------------------
# In-memory caches
# ---------------------------------------------------------------------------
_AUDIO_CACHE: dict[str, tuple[float, dict]] = {}
_AUDIO_CACHE_TTL = 1800   # 30 min (URLs from InnerTube are valid ~6 hrs)

_SEARCH_CACHE: dict[str, tuple[float, list]] = {}
_SEARCH_CACHE_TTL = 300   # 5 min

STREAM_CACHE: dict[str, dict] = {}   # only used for proxied URLs

# ---------------------------------------------------------------------------
# ─────────────────────────── METHOD 1: INNERTUBE ──────────────────────────
# Direct call to YouTube's private player API. No yt-dlp. ~0.3–0.8 seconds.
# ---------------------------------------------------------------------------
_INNERTUBE_CLIENTS = [
    # tv_embedded: embedded TV client — NOT subject to bot-guard on datacenter IPs
    {
        "name": "tv_embedded",
        "key": "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8",
        "context": {
            "client": {
                "clientName": "TVHTML5_SIMPLY_EMBEDDED_PLAYER",
                "clientVersion": "2.0",
                "hl": "en",
            },
            "thirdParty": {"embedUrl": "https://www.youtube.com/"},
        },
    },
    # android_embedded: bot-guard exempt client
    {
        "name": "android_embedded",
        "key": "AIzaSyA8eiZmM1FaDVjRy-df2KTyQ_vz_yYM39w",
        "context": {
            "client": {
                "clientName": "ANDROID_EMBEDDED_PLAYER",
                "clientVersion": "19.09.37",
                "androidSdkVersion": 30,
                "hl": "en",
            }
        },
    },
    # ios_embedded
    {
        "name": "ios_embedded",
        "key": "AIzaSyB-63vPrdThhKuerbB2N_l7Kwwcxj6yUA",
        "context": {
            "client": {
                "clientName": "IOS_MESSAGES_EXTENSION",
                "clientVersion": "19.09.3",
                "deviceModel": "iPhone14,3",
                "hl": "en",
            }
        },
    },
    # web_embedded — final InnerTube fallback
    {
        "name": "web_embedded",
        "key": "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8",
        "context": {
            "client": {
                "clientName": "WEB_EMBEDDED_PLAYER",
                "clientVersion": "2.20231213.01.00",
                "hl": "en",
            },
            "thirdParty": {"embedUrl": "https://www.youtube.com/"},
        },
    },
]

# Keep signatureTimestamp up to date — can be fetched from
# https://www.youtube.com/iframe_api but a recent static value works fine.
_SIG_TIMESTAMP = 20100

_INNERTUBE_URL = "https://www.youtube.com/youtubei/v1/player"
_INNERTUBE_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 Chrome/91.0.4472.120 Mobile Safari/537.36",
    "X-YouTube-Client-Name": "56",
    "X-YouTube-Client-Version": "2.0",
    "Origin": "https://www.youtube.com",
    "Referer": "https://www.youtube.com/",
}


def _innertube_extract(video_id: str) -> dict | None:
    """Call YouTube InnerTube player API directly. Returns audio info or None."""
    for client in _INNERTUBE_CLIENTS:
        try:
            payload = {
                "videoId": video_id,
                "context": client["context"],
                "params": "CgIIAQ==",   # audio-only hint
                "playbackContext": {
                    "contentPlaybackContext": {"signatureTimestamp": _SIG_TIMESTAMP}
                },
            }
            t0 = time.time()
            r = httpx.post(
                _INNERTUBE_URL,
                params={"key": client["key"], "prettyPrint": "false"},
                headers=_INNERTUBE_HEADERS,
                json=payload,
                timeout=10,
                follow_redirects=True,
            )
            elapsed = round(time.time() - t0, 2)

            if r.status_code != 200:
                log.warning(f"InnerTube {client['name']} HTTP {r.status_code} for {video_id}")
                continue

            data = r.json()

            # Check for playability
            status = data.get("playabilityStatus", {}).get("status", "")
            if status not in ("OK", ""):
                reason = data.get("playabilityStatus", {}).get("reason", status)
                log.warning(f"InnerTube {client['name']} not playable: {reason}")
                continue

            streaming = data.get("streamingData") or {}
            adaptive = streaming.get("adaptiveFormats") or []
            combined = streaming.get("formats") or []

            # Prefer audio-only adaptive formats (highest quality first)
            audio_fmts = [
                f for f in adaptive
                if f.get("mimeType", "").startswith("audio/") and f.get("url")
            ]
            # Sort by bitrate desc
            audio_fmts.sort(key=lambda f: f.get("averageBitrate") or f.get("bitrate") or 0, reverse=True)

            if not audio_fmts:
                # Fall back to combined formats
                audio_fmts = [f for f in combined if f.get("url")]

            if not audio_fmts:
                log.warning(f"InnerTube {client['name']} no audio formats for {video_id}")
                continue

            best = audio_fmts[0]
            audio_url = best.get("url")
            if not audio_url:
                continue

            # Get metadata
            details = data.get("videoDetails") or {}
            thumbs = details.get("thumbnail", {}).get("thumbnails") or []
            thumbnail = thumbs[-1]["url"] if thumbs else f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"

            log.info(f"InnerTube {client['name']} SUCCESS for {video_id} in {elapsed}s")
            return {
                "url": audio_url,
                "title": details.get("title", ""),
                "thumbnail": thumbnail,
                "duration": int(details.get("lengthSeconds") or 0) or None,
                "http_headers": {
                    "User-Agent": "com.google.android.youtube/19.09.37 (Linux; U; Android 11)",
                },
                "direct": True,  # URL is direct, no proxy needed
                "expires_in": int(streaming.get("expiresInSeconds") or 21600),
            }

        except Exception as e:
            log.warning(f"InnerTube {client['name']} error for {video_id}: {e}")
            continue

    return None


# ---------------------------------------------------------------------------
# ──────────────────────── METHOD 2: INVIDIOUS ─────────────────────────────
# ---------------------------------------------------------------------------
_INVIDIOUS = [
    "https://invidious.privacydev.net",
    "https://yt.drgnz.club",
    "https://iv.datura.network",
    "https://invidious.fdn.fr",
]


def _invidious_extract(video_id: str) -> dict | None:
    for base in _INVIDIOUS:
        try:
            r = httpx.get(
                f"{base}/api/v1/videos/{video_id}",
                params={"fields": "title,lengthSeconds,videoThumbnails,adaptiveFormats,formatStreams"},
                timeout=6,
                follow_redirects=True,
            )
            if r.status_code != 200:
                continue
            data = r.json()
            fmts = data.get("adaptiveFormats") or []
            audio = [f for f in fmts if f.get("type", "").startswith("audio/") and f.get("url")]
            if not audio:
                audio = [f for f in (data.get("formatStreams") or []) if f.get("url")]
            if not audio:
                continue
            audio.sort(key=lambda f: int(f.get("bitrate") or 0), reverse=True)
            best = audio[0]
            thumbs = data.get("videoThumbnails") or []
            thumb = thumbs[0]["url"] if thumbs else f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
            log.info(f"Invidious {base} SUCCESS for {video_id}")
            return {
                "url": best["url"],
                "title": data.get("title", ""),
                "thumbnail": thumb,
                "duration": int(data.get("lengthSeconds") or 0) or None,
                "http_headers": {},
                "direct": True,
            }
        except Exception as e:
            log.warning(f"Invidious {base} failed for {video_id}: {e}")
    return None


# ---------------------------------------------------------------------------
# ──────────────────────── METHOD 3: yt-dlp FALLBACK ───────────────────────
# ---------------------------------------------------------------------------
_YDL_STRATEGIES = [
    {"name": "tv_embedded", "args": {"youtube": {"player_client": ["tv_embedded"]}}},
    {"name": "android_vr",  "args": {"youtube": {"player_client": ["android_vr"]}}},
    {"name": "mweb",        "args": {"youtube": {"player_client": ["mweb"]}}},
]


def _ydl_base_opts(extractor_args: dict) -> dict:
    opts = {
        "format": "bestaudio[ext=m4a]/bestaudio/best",
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "nocheckcertificate": True,
        "cachedir": False,
        "socket_timeout": 15,
        "retries": 1,
        "extractor_args": extractor_args,
        "http_headers": {
            "User-Agent": "com.google.android.youtube/19.09.37 (Linux; U; Android 11)",
        },
    }
    cp = _cookie_path()
    if cp:
        opts["cookiefile"] = cp
    return opts


def _cookie_path() -> str | None:
    cf = os.environ.get("YT_COOKIES_FILE")
    if cf and os.path.exists(cf):
        return cf
    if os.path.exists("cookies.txt"):
        return os.path.abspath("cookies.txt")
    return None


def _pick_url(info: dict) -> str | None:
    u = info.get("url")
    if u:
        return u
    fmts = info.get("formats") or []
    ao = [f for f in fmts if f.get("url") and f.get("vcodec") in (None, "none", "") and f.get("acodec") not in (None, "none", "")]
    aa = [f for f in fmts if f.get("url") and f.get("acodec") not in (None, "none", "")]
    af = [f for f in fmts if f.get("url")]
    c = (ao or aa or af or [None])[0]
    return c["url"] if c else None


def _ytdlp_extract(video_id: str) -> dict | None:
    url = f"https://www.youtube.com/watch?v={video_id}"
    last_err = None
    for strat in _YDL_STRATEGIES:
        try:
            log.info(f"yt-dlp {strat['name']} for {video_id}")
            with yt_dlp.YoutubeDL(_ydl_base_opts(strat["args"])) as ydl:
                info = ydl.extract_info(url, download=False)
            audio = _pick_url(info)
            if audio:
                log.info(f"yt-dlp {strat['name']} SUCCESS for {video_id}")
                return {
                    "url": audio,
                    "title": info.get("title", ""),
                    "thumbnail": info.get("thumbnail") or f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
                    "duration": info.get("duration"),
                    "http_headers": info.get("http_headers") or {},
                    "direct": False,
                }
        except Exception as e:
            last_err = e
            log.warning(f"yt-dlp {strat['name']} FAILED for {video_id}: {e}")
    if last_err:
        raise last_err
    return None


# ---------------------------------------------------------------------------
# ─────────────────────── UNIFIED EXTRACTION ───────────────────────────────
# ---------------------------------------------------------------------------
def _extract(video_id: str) -> dict:
    # 1. Cache
    cached = _AUDIO_CACHE.get(video_id)
    if cached and time.time() - cached[0] < _AUDIO_CACHE_TTL:
        log.info(f"Cache HIT {video_id}")
        return cached[1]

    song = None

    # 2. InnerTube (fastest)
    song = _innertube_extract(video_id)

    # 3. Invidious
    if not song:
        song = _invidious_extract(video_id)

    # 4. yt-dlp (slow fallback)
    if not song:
        song = _ytdlp_extract(video_id)

    if not song:
        raise RuntimeError(f"All extraction methods failed for {video_id}")

    _AUDIO_CACHE[video_id] = (time.time(), song)
    if len(_AUDIO_CACHE) > 150:
        oldest = min(_AUDIO_CACHE, key=lambda k: _AUDIO_CACHE[k][0])
        _AUDIO_CACHE.pop(oldest, None)

    return song


# ---------------------------------------------------------------------------
# ─────────────────────── FAST SEARCH ──────────────────────────────────────
# ---------------------------------------------------------------------------
_INNERTUBE_SEARCH_URL = "https://www.youtube.com/youtubei/v1/search"

_INNERTUBE_SEARCH_CLIENTS = [
    {
        "name": "android",
        "key": "AIzaSyA8eiZmM1FaDVjRy-df2KTyQ_vz_yYM39w",
        "context": {
            "client": {
                "clientName": "ANDROID",
                "clientVersion": "19.09.37",
                "androidSdkVersion": 30,
                "hl": "en",
                "gl": "US",
            }
        },
    },
    {
        "name": "web",
        "key": "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8",
        "context": {
            "client": {
                "clientName": "WEB",
                "clientVersion": "2.20240101.00.00",
                "hl": "en",
                "gl": "US",
            }
        },
    },
]

_INNERTUBE_SEARCH_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "com.google.android.youtube/19.09.37 (Linux; U; Android 11)",
    "Origin": "https://www.youtube.com",
    "Referer": "https://www.youtube.com/",
}


def _collect_video_renderers(node, out: list, limit: int):
    """Recursively walk InnerTube JSON collecting every videoRenderer dict.
    Robust against nested-structure changes between clients/versions."""
    if len(out) >= limit or not isinstance(node, (dict, list)):
        return
    if isinstance(node, dict):
        vr = node.get("videoRenderer")
        if isinstance(vr, dict) and vr.get("videoId"):
            out.append(vr)
            if len(out) >= limit:
                return
        for v in node.values():
            _collect_video_renderers(v, out, limit)
    else:
        for item in node:
            _collect_video_renderers(item, out, limit)


def _parse_duration_text(text):
    if not text:
        return None
    try:
        secs = 0
        for part in str(text).strip().split(":"):
            secs = secs * 60 + int(part)
        return secs if secs > 0 else None
    except Exception:
        return None


def _innertube_search(query: str, limit: int = 8) -> list[dict]:
    """Direct InnerTube search API — ~0.3-0.8s, no yt-dlp overhead."""
    for client in _INNERTUBE_SEARCH_CLIENTS:
        try:
            payload = {"context": client["context"], "query": query}
            t0 = time.time()
            r = httpx.post(
                _INNERTUBE_SEARCH_URL,
                params={"key": client["key"], "prettyPrint": "false"},
                headers=_INNERTUBE_SEARCH_HEADERS,
                json=payload,
                timeout=8,
                follow_redirects=True,
            )
            elapsed = round(time.time() - t0, 2)
            if r.status_code != 200:
                log.warning(f"InnerTube search {client['name']} HTTP {r.status_code} for '{query}'")
                continue
            data = r.json()
            renderers: list = []
            _collect_video_renderers(data, renderers, limit + 4)
            if not renderers:
                log.warning(f"InnerTube search {client['name']} no renderers for '{query}'")
                continue
            results = []
            for vr in renderers:
                vid = vr.get("videoId")
                if not vid:
                    continue
                title_runs = (vr.get("title") or {}).get("runs") or []
                title = "".join(seg.get("text", "") for seg in title_runs) or "Unknown"
                thumbs = (vr.get("thumbnail") or {}).get("thumbnails") or []
                thumb = thumbs[-1]["url"] if thumbs else f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
                length_text = (vr.get("lengthText") or {}).get("simpleText") or ""
                dur = _parse_duration_text(length_text)
                byline_runs = ((vr.get("ownerText") or {}).get("runs")) or ((vr.get("shortBylineText") or {}).get("runs")) or []
                channel = (byline_runs[0].get("text", "") if byline_runs else "") or ""
                results.append({
                    "video_id": vid,
                    "title": title,
                    "thumbnail": thumb,
                    "duration": dur,
                    "channel": channel or "YouTube Music",
                })
                if len(results) >= limit:
                    break
            if results:
                log.info(f"InnerTube search {client['name']} SUCCESS '{query}' -> {len(results)} in {elapsed}s")
                return results
        except Exception as e:
            log.warning(f"InnerTube search {client['name']} error for '{query}': {e}")
            continue
    return []


def _ytdlp_search(query: str, limit: int = 8) -> list[dict]:
    """yt-dlp flat search — reliable fallback when InnerTube search returns nothing."""
    opts = {
        "extract_flat": "in_playlist",
        "quiet": True,
        "no_warnings": True,
        "nocheckcertificate": True,
        "socket_timeout": 12,
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
            if "/" in vid:
                vid = vid.split("/")[-1].split("=")[-1]
            thumb = e.get("thumbnail") or (f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg" if vid else "")
            results.append({
                "video_id": vid,
                "title": e.get("title", "Unknown"),
                "thumbnail": thumb,
                "duration": e.get("duration"),
                "channel": e.get("channel") or e.get("uploader") or "",
            })
        return results
    except Exception as e:
        log.error(f"yt-dlp search error '{query}': {e}")
        return []


def _search_metadata(query: str, limit: int = 8) -> list[dict]:
    key = f"{query}:{limit}"
    c = _SEARCH_CACHE.get(key)
    if c and time.time() - c[0] < _SEARCH_CACHE_TTL:
        return c[1]

    # 1. Fast InnerTube search (~0.5s)
    results = _innertube_search(query, limit)
    # 2. Fallback to yt-dlp (~2-4s) so search always returns something
    if not results:
        results = _ytdlp_search(query, limit)

    if results:
        _SEARCH_CACHE[key] = (time.time(), results)
        if len(_SEARCH_CACHE) > 200:
            oldest = min(_SEARCH_CACHE, key=lambda k: _SEARCH_CACHE[k][0])
            _SEARCH_CACHE.pop(oldest, None)
    return results


# ---------------------------------------------------------------------------
# ─────────────────────── HELPERS ──────────────────────────────────────────
# ---------------------------------------------------------------------------
def _related_query(title: str) -> str:
    clean = re.sub(r'\(.*?\)|\[.*?\]|official|video|audio|lyrics|hd|4k|remaster|ft\.?|feat\.?', '', title, flags=re.I).strip()
    return " ".join(clean.split()[:4]) + " similar song"


def _build_response(song: dict, base_url: str) -> dict:
    audio_url = song.get("url")
    if not audio_url:
        raise ValueError("No audio URL")

    # For direct URLs (InnerTube/Invidious), return URL directly — no proxy needed
    # This is WAY faster since the Android client fetches from YouTube CDN directly
    if song.get("direct"):
        return {
            "title": song.get("title"),
            "thumbnail": song.get("thumbnail"),
            "duration": song.get("duration"),
            "audio_url": audio_url,  # Direct YouTube CDN URL
        }

    # For yt-dlp URLs that may need proxying
    req_id = str(uuid.uuid4())
    STREAM_CACHE[req_id] = {
        "url": audio_url,
        "headers": song.get("http_headers") or {},
        "title": song.get("title"),
        "thumbnail": song.get("thumbnail"),
        "duration": song.get("duration"),
    }
    if len(STREAM_CACHE) > 400:
        oldest = next(iter(STREAM_CACHE))
        STREAM_CACHE.pop(oldest, None)
    return {
        "title": song.get("title"),
        "thumbnail": song.get("thumbnail"),
        "duration": song.get("duration"),
        "audio_url": f"{base_url.rstrip('/')}/stream?id={req_id}",
    }


# ---------------------------------------------------------------------------
# ─────────────────────── API ROUTES ───────────────────────────────────────
# ---------------------------------------------------------------------------
@app.get("/")
def root():
    return {"status": "ok", "version": "4.1", "method": "innertube+invidious+ytdlp"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/wake")
def wake():
    """Lightweight ping to keep Render server warm — call this on app open."""
    return {"status": "awake"}


@app.get("/debug")
def debug():
    try:
        ver = yt_dlp.version.__version__
    except Exception:
        ver = "unknown"
    return {
        "yt_dlp_version": ver,
        "audio_cache": len(_AUDIO_CACHE),
        "search_cache": len(_SEARCH_CACHE),
        "stream_cache": len(STREAM_CACHE),
    }


@app.get("/test_extract")
def test_extract(v: str = "dQw4w9WgXcQ"):
    """Test all extraction methods for a video ID."""
    results = []

    # Test InnerTube
    for client in _INNERTUBE_CLIENTS:
        try:
            t0 = time.time()
            payload = {
                "videoId": v,
                "context": client["context"],
                "params": "CgIIAQ==",
            }
            r = httpx.post(
                _INNERTUBE_URL,
                params={"key": client["key"], "prettyPrint": "false"},
                headers=_INNERTUBE_HEADERS,
                json=payload,
                timeout=8,
            )
            data = r.json()
            status = data.get("playabilityStatus", {}).get("status", "UNKNOWN")
            fmts = data.get("streamingData", {}).get("adaptiveFormats") or []
            audio = [f for f in fmts if f.get("mimeType", "").startswith("audio/") and f.get("url")]
            elapsed = round(time.time() - t0, 2)
            results.append({
                "method": f"innertube:{client['name']}",
                "success": len(audio) > 0,
                "playability": status,
                "audio_formats": len(audio),
                "elapsed_s": elapsed,
            })
            if audio:
                break
        except Exception as e:
            results.append({"method": f"innertube:{client['name']}", "success": False, "error": str(e)[:100]})

    return {"video_id": v, "results": results}


@app.get("/search_results")
def search_results(query: str, limit: int = 8):
    if not query.strip():
        raise HTTPException(400, "query required")
    results = _search_metadata(query.strip(), min(limit, 12))
    if not results:
        results = _search_metadata(f"{query.strip()} music", min(limit, 12))
    return {"results": results}


@app.get("/play")
def play_song(video_id: str, request: Request):
    if not video_id.strip():
        raise HTTPException(400, "video_id required")
    try:
        song = _extract(video_id.strip())
    except Exception as e:
        log.error(f"/play {video_id}: {e}")
        raise HTTPException(502, str(e))
    try:
        return _build_response(song, str(request.base_url))
    except ValueError as e:
        raise HTTPException(502, str(e))


@app.get("/search")
def search_song(query: str, request: Request):
    if not query.strip():
        raise HTTPException(400, "query required")
    q = query.strip()

    # Get video_id from fast search first
    results = _search_metadata(f"{q} song", 1) or _search_metadata(q, 1)
    if results and results[0].get("video_id"):
        vid = results[0]["video_id"]
        try:
            song = _extract(vid)
            return _build_response(song, str(request.base_url))
        except Exception as e:
            log.warning(f"/search extract {vid}: {e}")

    # Last resort: yt-dlp search+extract
    for strat in _YDL_STRATEGIES:
        try:
            with yt_dlp.YoutubeDL(_ydl_base_opts(strat["args"])) as ydl:
                info = ydl.extract_info(f"ytsearch1:{q} song", download=False)
            entries = info.get("entries") or []
            if entries:
                entry = entries[0]
                audio = _pick_url(entry)
                if audio:
                    song = {
                        "url": audio, "title": entry.get("title", q),
                        "thumbnail": entry.get("thumbnail") or "",
                        "duration": entry.get("duration"),
                        "http_headers": entry.get("http_headers") or {},
                        "direct": False,
                    }
                    return _build_response(song, str(request.base_url))
        except Exception as e:
            log.warning(f"/search yt-dlp {strat['name']}: {e}")

    raise HTTPException(502, "Could not extract audio")


@app.get("/related")
def related_song(title: str, request: Request):
    rel_q = _related_query(title)
    results = _search_metadata(rel_q, 1)
    if results and results[0].get("video_id"):
        vid = results[0]["video_id"]
        try:
            song = _extract(vid)
            return _build_response(song, str(request.base_url))
        except Exception as e:
            log.warning(f"/related {vid}: {e}")
    raise HTTPException(502, "No related song found")


@app.get("/stream")
def stream_song(id: str, request: Request):
    entry = STREAM_CACHE.get(id)
    if not entry or not entry.get("url"):
        raise HTTPException(404, "Stream expired — search again.")
    url = entry["url"]
    headers = dict(entry.get("headers") or {})
    rng = request.headers.get("range")
    if rng:
        headers["range"] = rng
    for h in ("host", "Host", "connection", "Connection"):
        headers.pop(h, None)
    if "User-Agent" not in headers:
        headers["User-Agent"] = "com.google.android.youtube/19.09.37 (Linux; U; Android 11)"

    client = httpx.Client(follow_redirects=True, timeout=httpx.Timeout(30.0, read=None))
    try:
        upstream = client.send(client.build_request("GET", url, headers=headers), stream=True)
    except Exception as e:
        client.close()
        raise HTTPException(502, f"Stream connect failed: {e}")
    if upstream.status_code >= 400:
        code = upstream.status_code
        upstream.close(); client.close()
        raise HTTPException(code, "Stream rejected by provider")

    content_type = upstream.headers.get("content-type", "audio/mp4")

    def _iter():
        try:
            for chunk in upstream.iter_bytes(chunk_size=65536):
                if chunk:
                    yield chunk
        finally:
            upstream.close(); client.close()

    resp = StreamingResponse(_iter(), media_type=content_type, status_code=upstream.status_code)
    for h in ("content-range", "content-length", "accept-ranges"):
        if h in upstream.headers:
            resp.headers[h] = upstream.headers[h]
    resp.headers["accept-ranges"] = "bytes"
    return resp
