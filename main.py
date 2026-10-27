"""
MyMusic Backend v5.0 — INSTANT Dedicated Music API + YouTube Fallback
Primary Engine:
  JioSaavn Dedicated Music API (~0.1–0.3s)
  - Direct 320kbps / 160kbps AAC/MP4 audio CDN stream URLs
  - High-res 500x500 album art
  - Zero bot detection, zero tokens, zero cookies, zero IP blocking
  - Instant direct CDN streaming (no Render proxy overhead/buffering)
Fallback Engine:
  - Smart YouTube oEmbed bridge: Converts YouTube IDs to Saavn 320kbps streams
  - Invidious public API fallback
  - yt-dlp fallback (last resort)
"""
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

import os
import re
import uuid
import time
import json
import html
import base64
import logging
import httpx
try:
    import yt_dlp
except ImportError:
    yt_dlp = None
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, RedirectResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("mymusic")

app = FastAPI(title="MyMusic Backend", version="5.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Optional cookies for fallback yt-dlp
_YT_COOKIES_ENV = os.environ.get("YT_COOKIES", "").strip()
if _YT_COOKIES_ENV:
    try:
        _content = _YT_COOKIES_ENV
        if "# Netscape" not in _content and "\t" not in _content:
            try:
                _content = base64.b64decode(_YT_COOKIES_ENV).decode("utf-8", errors="ignore")
            except Exception:
                pass
        with open("cookies.txt", "w", newline="\n") as _f:
            _f.write(_content)
        log.info("Materialized YT_COOKIES env var into cookies.txt")
    except Exception as _e:
        log.warning(f"Failed to materialize YT_COOKIES: {_e}")

# ---------------------------------------------------------------------------
# Caches
# ---------------------------------------------------------------------------
# Pre-cached song audio metadata (song_id -> dict with audio_url, title, etc.)
_SAAVN_MEDIA_CACHE: dict[str, dict] = {}

# Search query cache
_SEARCH_CACHE: dict[str, tuple[float, list]] = {}
_SEARCH_CACHE_TTL = 600   # 10 min

# Fallback audio cache
_AUDIO_CACHE: dict[str, tuple[float, dict]] = {}
_AUDIO_CACHE_TTL = 1800   # 30 min

# Legacy proxy stream cache
STREAM_CACHE: dict[str, dict] = {}

# ---------------------------------------------------------------------------
# ────────────────────── METHOD 1: DEDICATED MUSIC API ───────────────────────
# ---------------------------------------------------------------------------
_SAAVN_API_URL = "https://www.jiosaavn.com/api.php"
_SAAVN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}
_DES_KEY = b"38346591"


def _decrypt_saavn_url(enc_url: str) -> str:
    """Decrypt JioSaavn media URL using DES-ECB PKCS5."""
    if not enc_url:
        return ""
    try:
        raw = base64.b64decode(enc_url)
    except Exception:
        return ""

    # 1. Try pyDes (pure-python, ultra-reliable)
    try:
        import pyDes
        cipher = pyDes.des(_DES_KEY, pyDes.ECB, padmode=pyDes.PAD_PKCS5)
        return cipher.decrypt(raw).decode("utf-8")
    except Exception:
        pass

    # 2. Try cryptography
    try:
        from cryptography.hazmat.primitives.ciphers import Cipher, modes
        from cryptography.hazmat.primitives import padding
        try:
            from cryptography.hazmat.decrepit.ciphers.algorithms import TripleDES
        except ImportError:
            from cryptography.hazmat.primitives.ciphers.algorithms import TripleDES
        cipher = Cipher(TripleDES(_DES_KEY * 3), modes.ECB())
        decryptor = cipher.decryptor()
        dec = decryptor.update(raw) + decryptor.finalize()
        unpadder = padding.PKCS7(64).unpadder()
        return (unpadder.update(dec) + unpadder.finalize()).decode("utf-8")
    except Exception as e:
        log.error(f"DES decryption failed: {e}")
        return ""


def _format_stream_url(decrypted_url: str, has_320kbps: bool = True) -> str:
    """Upgrade audio stream quality to 320kbps or 160kbps."""
    if not decrypted_url:
        return ""
    if "_96.mp4" in decrypted_url:
        if has_320kbps:
            return decrypted_url.replace("_96.mp4", "_320.mp4")
        return decrypted_url.replace("_96.mp4", "_160.mp4")
    return decrypted_url


def _format_thumbnail(img_url: str) -> str:
    """Upgrade album artwork to crisp 500x500 resolution."""
    if not img_url:
        return ""
    return re.sub(r"-(150x150|50x50)\.", "-500x500.", img_url)


def _saavn_search(query: str, limit: int = 8) -> list[dict]:
    """Instant search returning song metadata and pre-caching audio stream URLs."""
    try:
        t0 = time.time()
        params = {
            "__call": "search.getResults",
            "_format": "json",
            "_marker": "0",
            "cc": "in",
            "includeMetaTags": "1",
            "q": query,
            "n": max(limit, 8),
            "p": 1,
        }
        r = httpx.get(_SAAVN_API_URL, params=params, headers=_SAAVN_HEADERS, timeout=6.0)
        if r.status_code != 200:
            log.warning(f"Saavn search HTTP {r.status_code} for '{query}'")
            return []

        data = r.json()
        raw_results = data.get("results") or []
        if not raw_results:
            return []

        results = []
        for s in raw_results:
            song_id = s.get("id")
            if not song_id:
                continue

            raw_title = s.get("song") or s.get("title") or "Unknown"
            title = html.unescape(raw_title).strip()

            raw_artist = s.get("primary_artists") or s.get("singers") or s.get("music") or "Artist"
            artist = html.unescape(raw_artist).strip()

            thumb = _format_thumbnail(s.get("image") or "")
            try:
                dur = int(s.get("duration") or 0) or None
            except Exception:
                dur = None

            enc_url = s.get("encrypted_media_url")
            has_320 = str(s.get("320kbps", "")).lower() == "true"
            audio_url = ""
            if enc_url:
                decrypted = _decrypt_saavn_url(enc_url)
                if decrypted:
                    audio_url = _format_stream_url(decrypted, has_320)

            # Store in media cache for instant /play resolution (<1ms)
            if audio_url:
                _SAAVN_MEDIA_CACHE[song_id] = {
                    "title": title,
                    "thumbnail": thumb,
                    "duration": dur,
                    "audio_url": audio_url,
                    "artist": artist,
                    "direct": True,
                }

            results.append({
                "video_id": song_id,
                "title": title,
                "thumbnail": thumb,
                "duration": dur,
                "channel": artist,
            })
            if len(results) >= limit:
                break

        elapsed = round(time.time() - t0, 3)
        log.info(f"Saavn search SUCCESS '{query}' -> {len(results)} songs in {elapsed}s")
        return results

    except Exception as e:
        log.warning(f"Saavn search error for '{query}': {e}")
        return []


def _saavn_extract_by_id(song_id: str) -> dict | None:
    """Extract 320kbps stream for a Saavn song ID."""
    cached = _SAAVN_MEDIA_CACHE.get(song_id)
    if cached and cached.get("audio_url"):
        log.info(f"Saavn memory cache HIT for {song_id}")
        return cached

    try:
        t0 = time.time()
        params = {
            "__call": "song.getDetails",
            "_format": "json",
            "cc": "in",
            "pids": song_id,
        }
        r = httpx.get(_SAAVN_API_URL, params=params, headers=_SAAVN_HEADERS, timeout=6.0)
        if r.status_code != 200:
            return None

        data = r.json()
        item = data.get(song_id)
        if not item or not isinstance(item, dict):
            return None

        title = html.unescape(item.get("song") or "Unknown").strip()
        artist = html.unescape(item.get("primary_artists") or item.get("singers") or "Artist").strip()
        thumb = _format_thumbnail(item.get("image") or "")
        try:
            dur = int(item.get("duration") or 0) or None
        except Exception:
            dur = None

        enc_url = item.get("encrypted_media_url")
        if not enc_url:
            return None

        has_320 = str(item.get("320kbps", "")).lower() == "true"
        decrypted = _decrypt_saavn_url(enc_url)
        if not decrypted:
            return None

        audio_url = _format_stream_url(decrypted, has_320)
        res = {
            "title": title,
            "thumbnail": thumb,
            "duration": dur,
            "audio_url": audio_url,
            "artist": artist,
            "direct": True,
        }
        _SAAVN_MEDIA_CACHE[song_id] = res
        log.info(f"Saavn song.getDetails SUCCESS {song_id} in {round(time.time() - t0, 3)}s")
        return res

    except Exception as e:
        log.warning(f"Saavn extract by id error for {song_id}: {e}")
        return None


# ---------------------------------------------------------------------------
# ──────────────── METHOD 2: SMART YOUTUBE-TO-SAAVN BRIDGE ──────────────────
# Resolves old YouTube video IDs from user history/liked songs to Saavn 320kbps.
# ---------------------------------------------------------------------------
def _clean_query_title(title: str) -> str:
    """Remove video noise like 'Official Video', '4K', 'Lyrics'."""
    t = re.sub(r'\(.*?(official|video|audio|lyrics|hd|4k|remaster|visualizer|feat\.?|ft\.?).*?\)', '', title, flags=re.I)
    t = re.sub(r'\[.*?(official|video|audio|lyrics|hd|4k|remaster|visualizer|feat\.?|ft\.?).*?\]', '', t, flags=re.I)
    t = re.sub(r'official\s+video|official\s+music\s+video|official\s+audio|lyric\s+video', '', t, flags=re.I)
    return " ".join(t.split()).strip()


def _extract_youtube_oembed_title(video_id: str) -> str | None:
    """Fetch video title via YouTube public oEmbed (never bot-blocked)."""
    try:
        r = httpx.get(
            f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json",
            timeout=4.0,
        )
        if r.status_code == 200:
            return r.json().get("title")
    except Exception:
        pass
    return None


def _resolve_youtube_id_via_saavn(video_id: str) -> dict | None:
    """Bridge a YouTube ID to a Saavn 320kbps direct stream."""
    yt_title = _extract_youtube_oembed_title(video_id)
    if not yt_title:
        return None

    cleaned = _clean_query_title(yt_title)
    log.info(f"Bridging YouTube ID {video_id} ('{yt_title}') -> Saavn query '{cleaned}'")
    results = _saavn_search(cleaned, limit=1)
    if results:
        top_id = results[0]["video_id"]
        song = _saavn_extract_by_id(top_id)
        if song:
            # Alias video_id in cache for subsequent instant plays
            _SAAVN_MEDIA_CACHE[video_id] = song
            return song
    return None


# ---------------------------------------------------------------------------
# ──────────────── METHOD 3: INVIDIOUS & yt-dlp FALLBACK ───────────────────
# ---------------------------------------------------------------------------
_INVIDIOUS = [
    "https://invidious.privacydev.net",
    "https://iv.datura.network",
    "https://yt.drgnz.club",
    "https://invidious.fdn.fr",
    "https://vid.puffyan.us",
]


def _invidious_extract(video_id: str) -> dict | None:
    for base in _INVIDIOUS:
        try:
            r = httpx.get(
                f"{base}/api/v1/videos/{video_id}",
                params={"fields": "title,lengthSeconds,videoThumbnails,adaptiveFormats,formatStreams"},
                timeout=5,
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
                "audio_url": best["url"],
                "title": data.get("title", ""),
                "thumbnail": thumb,
                "duration": int(data.get("lengthSeconds") or 0) or None,
                "http_headers": {},
                "direct": True,
            }
        except Exception:
            continue
    return None


_YDL_STRATEGIES = [
    {"name": "default",     "args": {}},
    {"name": "tv_embedded", "args": {"youtube": {"player_client": ["tv_embedded"]}}},
    {"name": "mweb",        "args": {"youtube": {"player_client": ["mweb"]}}},
]


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
    if yt_dlp is None:
        return None
    url = f"https://www.youtube.com/watch?v={video_id}"
    last_err = None
    opts = {
        "format": "bestaudio[ext=m4a]/bestaudio/best",
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "nocheckcertificate": True,
        "cachedir": False,
        "socket_timeout": 12,
        "retries": 1,
        "http_headers": {
            "User-Agent": "com.google.android.youtube/19.09.37 (Linux; U; Android 11)",
        },
    }
    cp = _cookie_path()
    if cp:
        opts["cookiefile"] = cp

    for strat in _YDL_STRATEGIES:
        try:
            curr_opts = dict(opts)
            curr_opts["extractor_args"] = strat["args"]
            with yt_dlp.YoutubeDL(curr_opts) as ydl:
                info = ydl.extract_info(url, download=False)
            audio = _pick_url(info)
            if audio:
                log.info(f"yt-dlp {strat['name']} SUCCESS for {video_id}")
                return {
                    "url": audio,
                    "audio_url": audio,
                    "title": info.get("title", ""),
                    "thumbnail": info.get("thumbnail") or f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
                    "duration": info.get("duration"),
                    "http_headers": info.get("http_headers") or {},
                    "direct": False,
                }
        except Exception as e:
            last_err = e
            log.warning(f"yt-dlp {strat['name']} failed for {video_id}: {e}")
    if last_err:
        raise last_err
    return None


# ---------------------------------------------------------------------------
# ────────────────────── UNIFIED EXTRACTION PIPELINE ─────────────────────────
# ---------------------------------------------------------------------------
def _extract(video_id: str) -> dict:
    # 1. In-memory Saavn cache (<1ms)
    cached = _SAAVN_MEDIA_CACHE.get(video_id)
    if cached and cached.get("audio_url"):
        return cached

    # 2. General audio cache
    ac = _AUDIO_CACHE.get(video_id)
    if ac and time.time() - ac[0] < _AUDIO_CACHE_TTL:
        return ac[1]

    # 3. Dedicated Music API (Saavn direct by ID)
    song = _saavn_extract_by_id(video_id)

    # 4. If not a Saavn ID, check if it's a YouTube ID and bridge via Saavn
    if not song and len(video_id) == 11:
        song = _resolve_youtube_id_via_saavn(video_id)

    # 5. Invidious fallback
    if not song:
        song = _invidious_extract(video_id)

    # 6. yt-dlp fallback (last resort)
    if not song:
        song = _ytdlp_extract(video_id)

    if not song:
        raise RuntimeError(f"Could not extract audio for {video_id}")

    _AUDIO_CACHE[video_id] = (time.time(), song)
    if len(_AUDIO_CACHE) > 200:
        oldest = min(_AUDIO_CACHE, key=lambda k: _AUDIO_CACHE[k][0])
        _AUDIO_CACHE.pop(oldest, None)

    return song


# ---------------------------------------------------------------------------
# ────────────────────── UNIFIED SEARCH PIPELINE ────────────────────────────
# ---------------------------------------------------------------------------
def _search_metadata(query: str, limit: int = 8) -> list[dict]:
    key = f"{query.lower().strip()}:{limit}"
    c = _SEARCH_CACHE.get(key)
    if c and time.time() - c[0] < _SEARCH_CACHE_TTL:
        return c[1]

    # 1. Primary: Dedicated Music API (Instant, ~0.1-0.3s)
    results = _saavn_search(query, limit)

    # 2. Secondary: If zero results, fallback to flat YouTube search
    if not results and yt_dlp is not None:
        log.info(f"Saavn 0 results for '{query}', falling back to yt-dlp search")
        opts = {
            "extract_flat": "in_playlist",
            "quiet": True,
            "no_warnings": True,
            "socket_timeout": 10,
        }
        cp = _cookie_path()
        if cp:
            opts["cookiefile"] = cp
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
            entries = info.get("entries") or []
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
                    "channel": e.get("channel") or e.get("uploader") or "YouTube Music",
                })
        except Exception as e:
            log.warning(f"Fallback search error '{query}': {e}")

    if results:
        _SEARCH_CACHE[key] = (time.time(), results)
        if len(_SEARCH_CACHE) > 300:
            oldest = min(_SEARCH_CACHE, key=lambda k: _SEARCH_CACHE[k][0])
            _SEARCH_CACHE.pop(oldest, None)

    return results


def _build_response(song: dict, base_url: str) -> dict:
    audio_url = song.get("audio_url") or song.get("url")
    if not audio_url:
        raise ValueError("No audio URL found for song")

    # Direct CDN URLs (Saavn, Invidious) — Android ExoPlayer streams directly from Akamai/CDN!
    # No proxy lag, zero Render bandwidth consumed, full byte-range seeking.
    if song.get("direct", True):
        return {
            "title": song.get("title", "Unknown"),
            "thumbnail": song.get("thumbnail", ""),
            "duration": song.get("duration"),
            "audio_url": audio_url,
        }

    # Proxied URLs (yt-dlp stream fallback)
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
        "title": song.get("title", "Unknown"),
        "thumbnail": song.get("thumbnail", ""),
        "duration": song.get("duration"),
        "audio_url": f"{base_url.rstrip('/')}/stream?id={req_id}",
    }


# ---------------------------------------------------------------------------
# ─────────────────────────── API ROUTES ───────────────────────────────────
# ---------------------------------------------------------------------------
@app.get("/")
def root():
    return {
        "status": "ok",
        "version": "5.0",
        "engine": "jiosaavn_instant+youtube_fallback",
        "latency": "instant (<0.3s)",
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/wake")
def wake():
    """Warm-up ping called on app open to wake Render instance."""
    return {"status": "awake"}


@app.get("/debug")
def debug():
    try:
        ver = yt_dlp.version.__version__ if yt_dlp else "not_installed"
    except Exception:
        ver = "unknown"
    return {
        "version": "5.0",
        "engine": "jiosaavn_instant+youtube_fallback",
        "yt_dlp_version": ver,
        "saavn_cache_size": len(_SAAVN_MEDIA_CACHE),
        "search_cache_size": len(_SEARCH_CACHE),
        "audio_cache_size": len(_AUDIO_CACHE),
    }


@app.get("/search_results")
def search_results(query: str, limit: int = 8):
    if not query or not query.strip():
        raise HTTPException(400, "query required")
    q = query.strip()
    results = _search_metadata(q, min(limit, 12))
    return {"results": results}


@app.get("/play")
def play_song(video_id: str, request: Request):
    if not video_id or not video_id.strip():
        raise HTTPException(400, "video_id required")
    vid = video_id.strip()
    try:
        song = _extract(vid)
        return _build_response(song, str(request.base_url))
    except Exception as e:
        log.error(f"/play error {vid}: {e}")
        raise HTTPException(502, f"Could not extract audio: {e}")


@app.get("/search")
def search_song(query: str, request: Request):
    """I'm Feeling Lucky — search + resolve audio in one instant shot."""
    if not query or not query.strip():
        raise HTTPException(400, "query required")
    q = query.strip()

    # 1. Instant search
    results = _search_metadata(q, limit=1)
    if results and results[0].get("video_id"):
        vid = results[0]["video_id"]
        try:
            song = _extract(vid)
            return _build_response(song, str(request.base_url))
        except Exception as e:
            log.warning(f"/search extract failed for {vid}: {e}")

    # 2. Fallback
    try:
        results = _search_metadata(f"{q} song", limit=1)
        if results and results[0].get("video_id"):
            song = _extract(results[0]["video_id"])
            return _build_response(song, str(request.base_url))
    except Exception:
        pass

    raise HTTPException(502, f"Could not find or extract song '{q}'")


@app.get("/related")
def related_song(title: str, request: Request):
    """Find and return a related song by artist/title."""
    if not title or not title.strip():
        raise HTTPException(400, "title required")

    clean = _clean_query_title(title.strip())
    # Extract artist or key terms
    parts = [p.strip() for p in clean.split("-") if p.strip()]
    rel_query = parts[0] if len(parts) > 1 else " ".join(clean.split()[:3])

    results = _search_metadata(rel_query, limit=5)
    for r in results:
        # Don't pick the exact same song
        if clean.lower() not in r.get("title", "").lower():
            try:
                song = _extract(r["video_id"])
                return _build_response(song, str(request.base_url))
            except Exception:
                continue

    # Fallback: pick any top result
    if results:
        try:
            song = _extract(results[0]["video_id"])
            return _build_response(song, str(request.base_url))
        except Exception:
            pass

    raise HTTPException(502, "No related song found")


@app.get("/stream")
def stream_song(id: str, request: Request):
    """Streaming proxy fallback for yt-dlp URLs that cannot be streamed directly."""
    entry = STREAM_CACHE.get(id)
    if not entry or not entry.get("url"):
        raise HTTPException(404, "Stream expired. Please search again.")
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
        upstream.close()
        client.close()
        raise HTTPException(code, "Stream rejected by provider")

    content_type = upstream.headers.get("content-type", "audio/mp4")

    def _iter():
        try:
            for chunk in upstream.iter_bytes(chunk_size=65536):
                if chunk:
                    yield chunk
        finally:
            upstream.close()
            client.close()

    resp = StreamingResponse(_iter(), media_type=content_type, status_code=upstream.status_code)
    for h in ("content-range", "content-length", "accept-ranges"):
        if h in upstream.headers:
            resp.headers[h] = upstream.headers[h]
    resp.headers["accept-ranges"] = "bytes"
    return resp
