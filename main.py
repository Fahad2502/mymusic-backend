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
import math
import json
import html
import base64
import logging
import concurrent.futures
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

app = FastAPI(title="MyMusic Backend", version="6.0")
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
    "Cookie": "L=english%2Chindi; geo=IN; gdpr_acceptance=true",
    "X-Forwarded-For": "49.36.0.1",
    "X-Real-IP": "49.36.0.1",
    "CF-Connecting-IP": "49.36.0.1",
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


def _tokens(text: str) -> list[str]:
    return re.findall(r"[a-zA-Z0-9]+", (text or "").lower())


def _compact(text: str) -> str:
    return "".join(_tokens(text))


def _parse_count(value) -> int:
    """Parse play/view counts from ints, numeric strings, or '12M views'."""
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    t = str(value).lower().replace(",", "").replace("views", "").replace("view", "").strip()
    if not t:
        return 0
    m = re.match(r"^([\d.]+)\s*([kmb])?", t)
    if m:
        n = float(m.group(1))
        mul = {None: 1, "k": 1_000, "m": 1_000_000, "b": 1_000_000_000}[m.group(2)]
        return int(n * mul)
    digits = re.sub(r"\D", "", t)
    return int(digits) if digits else 0


def _levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    if abs(len(a) - len(b)) > 18:
        return max(len(a), len(b))
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i]
        for j, cb in enumerate(b, 1):
            curr.append(min(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = curr
    return prev[-1]


def _similarity(a: str, b: str) -> float:
    """0..1 similarity that tolerates typos, missing spaces, and extra words."""
    ca, cb = _compact(a), _compact(b)
    if not ca or not cb:
        return 0.0
    if ca == cb:
        return 1.0
    if ca in cb or cb in ca:
        shorter, longer = (ca, cb) if len(ca) <= len(cb) else (cb, ca)
        return 0.72 + 0.28 * (len(shorter) / len(longer))
    ta, tb = set(_tokens(a)), set(_tokens(b))
    overlap = (len(ta & tb) / len(ta)) if ta else 0.0
    if min(len(ca), len(cb)) <= 36:
        lev = 1.0 - (_levenshtein(ca, cb) / max(len(ca), len(cb)))
        return max(overlap, lev)
    return overlap


def _yt_pin(video_id: str, title: str, channel: str, duration: int | None = None) -> dict:
    return {
        "video_id": video_id,
        "title": title,
        "thumbnail": f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
        "duration": duration,
        "channel": channel,
        "album": "",
        "play_count": 500_000_000,
        "source": "youtube",
        "official": True,
    }


# Famous songs people type as movie names, spellings, or half-remembered titles.
# Pinned rows are the real original recordings (official artist / label).
_CANONICAL_HITS = [
    {
        "match": ("afreenafreen", "afreenafrin", "afrenafreen", "afreencokestudio", "afreen"),
        "search": "Afreen Afreen Coke Studio Rahat Fateh Ali Khan Momina Mustehsan",
        "need": ("afreen",),
        "pin": ("coke studio", "momina", "rahat", "season 9", "season9"),
        "pinned": [
            _yt_pin(
                "kw4tT7SCmaY",
                "Afreen Afreen | Coke Studio Season 9 | Rahat Fateh Ali Khan & Momina Mustehsan",
                "Coke Studio Pakistan",
                390,
            ),
        ],
    },
    {
        "match": (
            "tajdareharam", "tajdarharam", "tajdareharm", "tajdareharamm",
            "tajdareharamcoke", "tajdar",
        ),
        "search": "Tajdar e Haram Coke Studio Atif Aslam",
        "need": ("tajdar",),
        "pin": ("haram", "coke studio", "atif"),
        "pinned": [
            _yt_pin(
                "a18py61_F_w",
                "Tajdar-e-Haram | Coke Studio Season 8 | Atif Aslam",
                "Coke Studio Pakistan",
                607,
            ),
        ],
    },
    {
        "match": ("teremastmast", "mastmastdonain", "teremastdonain", "mastmast"),
        "movie": ("dabangg", "dabang"),
        "search": "Tere Mast Mast Do Nain Rahat Fateh Ali Khan Dabangg",
        "need": ("mast", "nain"),
        "pin": ("dabangg", "dabang", "rahat", "shreya", "t-series", "tseries"),
        "pinned": [
            _yt_pin(
                "oyLVu753XJw",
                "Tere Mast Mast Do Nain | Dabangg | Rahat Fateh Ali Khan, Shreya Ghoshal",
                "T-Series",
                201,
            ),
        ],
    },
]


_FAMOUS_VOICES = (
    "rahat fateh ali", "nusrat fateh ali", "atif aslam", "momina mustehsan",
    "arijit singh", "shreya ghoshal", "sonu nigam", "kumar sanu", "udit narayan",
    "lata mangeshkar", "asha bhosle", "kishore kumar",
    "mohammed rafi", "mohammad rafi", "a. r. rahman", "ar rahman", "armaan malik",
    "jubin nautiyal", "neha kakkar", "diljit dosanjh", "abida parveen",
    "ali sethi", "quratulain", "qurat-ul-ain", "shafqat amanat", "ali zafar",
    "hadiqa kiani", "sajjad ali", "mohit chauhan", "sunidhi chauhan",
    "sukhwinder singh", "rekha bhardwaj", "palak muchhal", "shankar mahadevan",
    "coke studio", "t-series", "tseries", "yrf", "sony music", "zee music",
    "saregama", "tips official", "emi pakistan", "speed records", "vevo",
)


def _has_famous_voice(item: dict) -> bool:
    blob = f"{item.get('channel', '')} {item.get('title', '')} {item.get('album', '')}".lower()
    if item.get("official"):
        return True
    if any(v in blob for v in _FAMOUS_VOICES):
        return True
    ch = (item.get("channel") or "").lower()
    return ch.endswith("topic") or "official" in ch


def _canonical_for(query: str) -> dict | None:
    cq = _compact(query)
    if not cq:
        return None
    qlow = query.lower()
    for hit in _CANONICAL_HITS:
        keys = sorted(hit.get("match") or (), key=len, reverse=True)
        for m in keys:
            mc = _compact(m)
            if not mc:
                continue
            if mc in cq or (len(cq) >= 5 and cq in mc):
                return hit
        for movie in hit.get("movie") or ():
            mv = _compact(movie)
            if cq == mv or qlow.strip() in (movie, f"{movie} movie", f"{movie} songs"):
                return hit
    return None


def _looks_like_lyrics(query: str) -> bool:
    if _canonical_for(query):
        return False
    words = _tokens(query)
    # Real titles are often 4–6 words ("tere mast mast do nain"). Lyric lines are longer.
    return len(words) >= 8 or (len(query.strip()) >= 40 and len(words) >= 6)


def _asked_junk(query: str) -> bool:
    q = query.lower()
    return any(k in q for k in ("karaoke", "instrumental", "cover", "remix", "slowed", "reverb"))


def _is_junk_text(*parts: str) -> bool:
    blob = " ".join(p.lower() for p in parts if p)
    return any(k in blob for k in (
        "karaoke", "instrumental", "tribute", "backing track", "originally performed",
        "heartstrings", "ringtone", "whatsapp", "status video", "nightcore",
        "8d audio", "slowed + reverb", "slowed+reverb",
    ))


def _is_devotional_query(query: str) -> bool:
    q = query.lower()
    return any(k in q for k in (
        "qawwali", "qawali", "kaawali", "bhajan", "kirtan", "naat",
        "hamd", "ghazal", "devotional", "aarti", "sufi", "mehfil",
    ))


def _item_play_count(item: dict) -> int:
    more = item.get("more_info") if isinstance(item.get("more_info"), dict) else {}
    return _parse_count(
        item.get("play_count")
        or item.get("playCount")
        or more.get("play_count")
        or more.get("playCount")
        or 0
    )


def _yt_suggest(query: str) -> list[str]:
    """YouTube search suggestions — fixes typos and completes famous titles."""
    try:
        r = httpx.get(
            "https://suggestqueries.google.com/complete/search",
            params={"client": "firefox", "ds": "yt", "hl": "en", "gl": "in", "q": query},
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=2.5,
        )
        if r.status_code != 200:
            return []
        data = r.json()
        return [s for s in (data[1] or []) if isinstance(s, str) and s.strip()][:8]
    except Exception as e:
        log.warning(f"YT suggest failed for '{query}': {e}")
        return []


def _lrclib_resolve(query: str) -> list[dict]:
    """Resolve a lyric snippet / misspelled line into real track + artist names."""
    try:
        r = httpx.get(
            "https://lrclib.net/api/search",
            params={"q": query},
            timeout=3.5,
            headers={"User-Agent": "MyMusic/5.3"},
        )
        if r.status_code != 200:
            return []
        out = []
        seen = set()
        for item in r.json()[:8]:
            title = (item.get("trackName") or "").strip()
            artist = (item.get("artistName") or "").strip()
            if not title:
                continue
            key = (_compact(title), _compact(artist.split(",")[0]))
            if key in seen:
                continue
            seen.add(key)
            out.append({"title": title, "artist": artist})
        return out
    except Exception as e:
        log.warning(f"LRCLIB resolve failed for '{query}': {e}")
        return []


def _score_song(item: dict, query: str) -> float:
    """Rank Saavn hits by title match + popularity, without burying niche genres."""
    q = query.lower().strip()
    raw_title = item.get("song") or item.get("title") or ""
    title = html.unescape(raw_title).strip()
    raw_artist = item.get("primary_artists") or item.get("singers") or ""
    artist = html.unescape(raw_artist).strip()
    more = item.get("more_info") if isinstance(item.get("more_info"), dict) else {}
    album = html.unescape(str(item.get("album") or more.get("album") or "")).strip()
    plays = _item_play_count(item)

    score = 0.0
    sim = max(_similarity(q, title), _similarity(q, f"{title} {artist}") * 0.9)
    score += sim * 8_000_000_000
    score += min(math.log10(plays + 1), 10) * 700_000_000

    if _is_junk_text(title, album, artist) and not _asked_junk(q):
        score -= 50_000_000_000

    return score


def _saavn_map_item(s: dict) -> dict | None:
    """Normalize a Saavn song dict into our search result shape and cache audio."""
    more = s.get("more_info") if isinstance(s.get("more_info"), dict) else {}
    song_id = s.get("id")
    if not song_id:
        return None

    raw_title = s.get("song") or s.get("title") or "Unknown"
    title = html.unescape(raw_title).strip()
    raw_artist = (
        s.get("primary_artists")
        or more.get("primary_artists")
        or s.get("singers")
        or more.get("singers")
        or s.get("music")
        or "Artist"
    )
    artist = html.unescape(raw_artist).strip()
    thumb = _format_thumbnail(s.get("image") or more.get("image") or "")
    try:
        dur = int(s.get("duration") or more.get("duration") or 0) or None
    except Exception:
        dur = None

    enc_url = s.get("encrypted_media_url") or more.get("encrypted_media_url")
    has_320 = str(s.get("320kbps") or more.get("320kbps") or "").lower() == "true"
    audio_url = ""
    if enc_url:
        decrypted = _decrypt_saavn_url(enc_url)
        if decrypted:
            audio_url = _format_stream_url(decrypted, has_320)
    if audio_url:
        _SAAVN_MEDIA_CACHE[song_id] = {
            "title": title,
            "thumbnail": thumb,
            "duration": dur,
            "audio_url": audio_url,
            "artist": artist,
            "direct": True,
        }

    album = html.unescape(str(s.get("album") or more.get("album") or "")).strip()
    return {
        "video_id": f"s:{song_id}",
        "title": title,
        "thumbnail": thumb,
        "duration": dur,
        "channel": artist,
        "album": album,
        "play_count": _item_play_count(s),
        "source": "saavn",
    }


def _saavn_autocomplete(query: str) -> list[dict]:
    """Title-first autocomplete — much better at famous-song matching than generic search."""
    try:
        params = {
            "__call": "autocomplete.get",
            "_format": "json",
            "_marker": "0",
            "cc": "in",
            "includeMetaTags": "1",
            "query": query,
        }
        r = httpx.get(_SAAVN_API_URL, params=params, headers=_SAAVN_HEADERS, timeout=5.0)
        if r.status_code != 200:
            return []
        data = r.json()
        songs = (data.get("songs") or {}).get("data") or []
        out = []
        for s in songs:
            mapped = _saavn_map_item(s)
            if mapped:
                out.append(mapped)
        return out
    except Exception as e:
        log.warning(f"Saavn autocomplete error for '{query}': {e}")
        return []


def _saavn_search(query: str, limit: int = 8) -> list[dict]:
    """Instant search returning the most famous song first and pre-caching audio stream URLs."""
    try:
        t0 = time.time()
        auto_hits: list[dict] = []
        raw_results: list = []
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
                f_auto = ex.submit(_saavn_autocomplete, query)
                f_full = ex.submit(
                    httpx.get,
                    _SAAVN_API_URL,
                    params={
                        "__call": "search.getResults",
                        "_format": "json",
                        "_marker": "0",
                        "cc": "in",
                        "includeMetaTags": "1",
                        "q": query,
                        "n": max(limit * 4, 40),
                        "p": 1,
                    },
                    headers=_SAAVN_HEADERS,
                    timeout=6.0,
                )
                auto_hits = f_auto.result() or []
                r = f_full.result()
                if r.status_code == 200:
                    raw_results = (r.json().get("results") or [])
                else:
                    log.warning(f"Saavn search HTTP {r.status_code} for '{query}'")
        except Exception as e:
            log.warning(f"Saavn parallel search error for '{query}': {e}")

        if not raw_results and not auto_hits:
            return []

        raw_results.sort(key=lambda item: _score_song(item, query), reverse=True)

        results = []
        seen_ids = set()
        seen_pairs = set()

        def _add(mapped: dict | None):
            if not mapped:
                return
            vid = mapped["video_id"]
            if vid in seen_ids:
                return
            seen_ids.add(vid)
            title = mapped["title"]
            artist = mapped["channel"]
            norm_key = (
                re.sub(r"\(.*?\)", "", title).strip().lower(),
                artist.lower().split(",")[0].strip(),
            )
            if norm_key in seen_pairs:
                return
            seen_pairs.add(norm_key)
            results.append(mapped)

        for hit in auto_hits:
            _add(hit)
        for s in raw_results:
            _add(_saavn_map_item(s))
            if len(results) >= max(limit * 3, 24):
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
    """Remove video noise like 'Official Video', '4K', 'Lyrics', '8K Song'."""
    t = re.sub(r'\(.*?(official|video|audio|lyrics|hd|4k|8k|remaster|visualizer|feat\.?|ft\.?).*?\)', '', title, flags=re.I)
    t = re.sub(r'\[.*?(official|video|audio|lyrics|hd|4k|8k|remaster|visualizer|feat\.?|ft\.?).*?\]', '', t, flags=re.I)
    t = re.sub(r'official\s+video|official\s+music\s+video|official\s+audio|lyric\s+video|full\s+song|8k\s+song|full\s+video|video\s+song', '', t, flags=re.I)
    return " ".join(t.split()).strip()


def _titles_match(query_title: str, candidate_title: str) -> bool:
    """Verify Saavn title is genuinely the queried song, not a random false match."""
    q_words = set(w for w in re.findall(r"[a-zA-Z0-9]+", query_title.lower()) if len(w) > 2)
    c_words = set(w for w in re.findall(r"[a-zA-Z0-9]+", candidate_title.lower()) if len(w) > 2)
    if not q_words or not c_words:
        return False
    overlap = len(q_words & c_words)
    return (overlap / len(c_words)) >= 0.45


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
    """Bridge a YouTube ID to a Saavn 320kbps direct stream IF it's genuinely the same song."""
    yt_title = _extract_youtube_oembed_title(video_id)
    if not yt_title:
        return None

    cleaned = _clean_query_title(yt_title)
    log.info(f"Bridging YouTube ID {video_id} ('{yt_title}') -> Saavn query '{cleaned}'")
    results = _saavn_search(cleaned, limit=3)
    for cand in results:
        cand_title = cand.get("title", "")
        if _titles_match(cleaned, cand_title):
            song = _saavn_extract_by_id(cand["video_id"])
            if song:
                _SAAVN_MEDIA_CACHE[video_id] = song
                log.info(f"Bridge SUCCESS: YouTube '{yt_title}' -> Saavn '{song['title']}'")
                return song
    log.info(f"No legitimate Saavn match for YouTube ID {video_id} ('{yt_title}'). Falling through to native YouTube extraction.")
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
    {"name": "android",     "args": {"youtube": {"player_client": ["android"]}}},
    {"name": "ios",         "args": {"youtube": {"player_client": ["ios"]}}},
    {"name": "mweb",        "args": {"youtube": {"player_client": ["mweb"]}}},
    {"name": "tv_embedded", "args": {"youtube": {"player_client": ["tv_embedded"]}}},
    {"name": "default",     "args": {}},
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
    # Strip the "s:" prefix that search results use to namespace Saavn IDs
    saavn_id = None
    if video_id.startswith("s:"):
        saavn_id = video_id[2:]
        video_id = saavn_id

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
    if not song and saavn_id is None and len(video_id) == 11:
        song = _resolve_youtube_id_via_saavn(video_id)

    # 5. Invidious fallback (YouTube IDs only)
    if not song and saavn_id is None:
        song = _invidious_extract(video_id)

    # 6. yt-dlp fallback (YouTube IDs only, last resort)
    if not song and saavn_id is None:
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
_OFFICIAL_CHANNELS = (
    "coke studio", "cokestudio", "t-series", "tseries", "sony music", "zee music",
    "yrf", "tips official", "vevo", "speed records", "saregama", "shemaroo",
    "emi pakistan", "hum tv", "osman mir", "nfaak", "nusrat fateh ali",
    "t-series islamic", "t-series bhakti", "devotional", "rajshri",
)


def _unified_score(item: dict, query: str, resolved: list[dict] | None = None) -> float:
    """Rank so the famous original is first — including lyric / typo / qawwali searches."""
    t = item.get("title", "")
    c = item.get("channel", "")
    a = item.get("album", "")
    q = query.strip()
    src = item.get("source", "saavn")
    plays = _parse_count(item.get("play_count") or 0)
    dur = item.get("duration") or 0
    lyric_q = _looks_like_lyrics(q)

    title_sim = _similarity(q, t)
    artist_sim = _similarity(q, c)
    combo_sim = _similarity(q, f"{t} {c}")
    relevance = max(title_sim, combo_sim, artist_sim * 0.65)

    resolved_boost = 0.0
    if resolved:
        for track in resolved:
            rt = track.get("title", "")
            ra = track.get("artist", "")
            hit = max(_similarity(rt, t), _similarity(f"{rt} {ra}", f"{t} {c}"))
            if hit > resolved_boost:
                resolved_boost = hit

    score = 0.0
    if lyric_q:
        # People typed a line, not a title — trust lyric resolution + popularity.
        score += resolved_boost * 12_000_000_000
        score += relevance * 3_000_000_000
        score += min(math.log10(plays + 1), 10) * 1_100_000_000
    else:
        score += relevance * 10_000_000_000
        score += resolved_boost * 6_000_000_000
        score += min(math.log10(plays + 1), 10) * 900_000_000

    blob = f"{t} {c} {a}".lower()
    if _is_junk_text(t, a, c) and not _asked_junk(q):
        score -= 40_000_000_000
    if any(k in blob for k in ("whatsapp", "status", "shorts", "tiktok", "ringtone")):
        score -= 18_000_000_000
    if any(k in blob for k in ("slowed", "reverb", "nightcore")) and not _asked_junk(q):
        score -= 12_000_000_000

    is_official = any(label in c.lower() for label in _OFFICIAL_CHANNELS) or \
        c.lower().endswith("topic") or "official" in c.lower()
    if is_official:
        score += 6_000_000_000
    if any(k in t.lower() for k in (
        "official video", "official audio", "full video", "full song",
        "coke studio", "original",
    )):
        score += 2_000_000_000

    # Shorts / clips almost never are "the" song
    if dur and dur < 50:
        score -= 15_000_000_000
    # Allow long qawwalis / bhajans / live mehfils; trim random 2-hour dumps otherwise
    if dur and dur > 45 * 60 and not _is_devotional_query(q) and "live" not in q.lower():
        score -= 8_000_000_000

    # Prefer a real music catalog match when titles actually line up
    if src == "saavn" and relevance >= 0.45 and _has_famous_voice(item):
        score += 2_500_000_000
    elif src == "saavn" and relevance >= 0.5 and not _has_famous_voice(item):
        score -= 20_000_000_000
    # YouTube is stronger for lyric lines, qawwali, and anything Saavn ranks poorly
    if src == "youtube" and (lyric_q or _is_devotional_query(q)):
        score += 3_000_000_000

    if _has_famous_voice(item):
        score += 20_000_000_000
    elif relevance >= 0.55:
        score -= 18_000_000_000

    score += _canonical_boost(item, q)
    return score


def _canonical_boost(item: dict, query: str) -> float:
    """Huge boost only for the original famous recording — covers get a penalty."""
    hit = _canonical_for(query)
    if not hit:
        return 0.0
    blob = f"{item.get('title', '')} {item.get('channel', '')} {item.get('album', '')}".lower()
    cb = _compact(blob)
    if not all(_compact(n) in cb or n.lower() in blob for n in hit.get("need") or ()):
        return 0.0
    pinned_ids = {p.get("video_id") for p in hit.get("pinned") or ()}
    if item.get("video_id") in pinned_ids or item.get("official"):
        return 120_000_000_000.0
    has_pin = any(p.lower() in blob or _compact(p) in cb for p in hit.get("pin") or ())
    if has_pin and _has_famous_voice(item):
        return 90_000_000_000.0
    if _has_famous_voice(item):
        return 55_000_000_000.0
    # Same song title, unknown singer — this is the junk the user was seeing
    return -40_000_000_000.0


def _ytdlp_flat_search(query: str, limit: int = 8) -> list[dict]:
    """Fallback YouTube flat search if InnerTube API is blocked."""
    if yt_dlp is None:
        return []
    try:
        opts = {
            "extract_flat": "in_playlist",
            "quiet": True,
            "no_warnings": True,
            "socket_timeout": 8,
            "extractor_args": {"youtube": {"player_client": ["android", "ios", "mweb"]}},
        }
        cp = _cookie_path()
        if cp:
            opts["cookiefile"] = cp
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
        entries = info.get("entries") or []
        results = []
        for e in entries:
            vid = e.get("id") or e.get("url") or ""
            if "/" in vid:
                vid = vid.split("/")[-1].split("=")[-1]
            if not vid:
                continue
            thumb = e.get("thumbnail") or f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
            results.append({
                "video_id": vid,
                "title": e.get("title", "Unknown"),
                "thumbnail": thumb,
                "duration": e.get("duration"),
                "channel": e.get("channel") or e.get("uploader") or "YouTube Music",
                "album": "",
                "play_count": 0,
                "source": "youtube",
            })
        return results
    except Exception as e:
        log.warning(f"yt-dlp flat search error '{query}': {e}")
        return []


def _youtube_innertube_search(query: str, limit: int = 8) -> list[dict]:
    """Fast, unblocked YouTube search via public InnerTube WEB API with country/language targeting."""
    try:
        t0 = time.time()
        url = "https://www.youtube.com/youtubei/v1/search?prettyPrint=false"
        payload = {
            "context": {
                "client": {
                    "clientName": "WEB",
                    "clientVersion": "2.20240410.01.00",
                    "hl": "en",
                    "gl": "IN"
                }
            },
            "query": query
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Content-Type": "application/json",
        }
        r = httpx.post(url, json=payload, headers=headers, timeout=8.0)
        if r.status_code != 200:
            log.warning(f"YouTube InnerTube HTTP {r.status_code} for '{query}', falling back to yt-dlp flat search")
            return _ytdlp_flat_search(query, limit)
        data = r.json()
        results = []
        contents = (
            data.get("contents", {})
            .get("twoColumnSearchResultsRenderer", {})
            .get("primaryContents", {})
            .get("sectionListRenderer", {})
            .get("contents", [])
        )
        for section in contents:
            for item in section.get("itemSectionRenderer", {}).get("contents", []):
                vr = item.get("videoRenderer")
                if not vr:
                    continue
                vid = vr.get("videoId")
                if not vid:
                    continue
                title = "".join(x.get("text", "") for x in (vr.get("title", {}).get("runs") or [])).strip()
                if not title:
                    continue
                owner = vr.get("ownerText", {}) or vr.get("longBylineText", {}) or vr.get("shortBylineText", {})
                channel = "".join(x.get("text", "") for x in (owner.get("runs") or [])).strip() or "YouTube Music"
                dur_text = vr.get("lengthText", {}).get("simpleText") or ""
                dur_sec = None
                if dur_text:
                    parts = dur_text.split(":")
                    if len(parts) == 2:
                        dur_sec = int(parts[0]) * 60 + int(parts[1])
                    elif len(parts) == 3:
                        dur_sec = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                thumbs = vr.get("thumbnail", {}).get("thumbnails") or []
                thumb = thumbs[-1].get("url") if thumbs else f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
                view_text = (
                    (vr.get("viewCountText") or {}).get("simpleText")
                    or "".join(x.get("text", "") for x in ((vr.get("viewCountText") or {}).get("runs") or []))
                    or (vr.get("shortViewCountText") or {}).get("simpleText")
                    or ""
                )
                results.append({
                    "video_id": vid,
                    "title": title,
                    "thumbnail": thumb,
                    "duration": dur_sec,
                    "channel": channel,
                    "album": "",
                    "play_count": _parse_count(view_text),
                    "source": "youtube",
                })
                if len(results) >= max(limit * 3, 20):
                    break
        if not results:
            log.info(f"InnerTube returned 0 results for '{query}', falling back to yt-dlp flat search")
            return _ytdlp_flat_search(query, limit)
        log.info(f"YouTube InnerTube search '{query}' -> {len(results)} items in {round(time.time() - t0, 3)}s")
        return results
    except Exception as e:
        log.warning(f"YouTube InnerTube search failed for '{query}': {e}")
        return _ytdlp_flat_search(query, limit)


def _extra_search_queries(original: str, suggestions: list[str], resolved: list[dict]) -> list[str]:
    """Build follow-up searches for typos and lyric-to-title resolution."""
    extras: list[str] = []
    orig_c = _compact(original)
    canon = _canonical_for(original)
    if canon and _compact(canon["search"]) != orig_c:
        extras.append(canon["search"])
    for s in suggestions[:2]:
        if _compact(s) != orig_c and _similarity(original, s) >= 0.45:
            extras.append(s)
            break
    for track in resolved[:2]:
        title = track.get("title") or ""
        artist = (track.get("artist") or "").split(",")[0].strip()
        if not title:
            continue
        q = f"{title} {artist}".strip()
        if _compact(q) != orig_c:
            extras.append(q)
    # Unique, max 2 extra queries to keep latency tight
    seen = {orig_c}
    out = []
    for q in extras:
        key = _compact(q)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(q)
        if len(out) >= 2:
            break
    return out


def _search_metadata(query: str, limit: int = 8) -> list[dict]:
    key = f"v5.5:{query.lower().strip()}:{limit}"
    c = _SEARCH_CACHE.get(key)
    if c and time.time() - c[0] < _SEARCH_CACHE_TTL:
        return c[1]

    q = query.strip()
    canon = _canonical_for(q)
    primary = canon["search"] if canon else q
    want_lyrics = (not canon) and (_looks_like_lyrics(q) or len(_tokens(q)) >= 6)
    saavn_results: list[dict] = []
    yt_results: list[dict] = []
    suggestions: list[str] = []
    resolved: list[dict] = []

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
            f_saavn = ex.submit(_saavn_search, q, limit)
            f_yt = ex.submit(_youtube_innertube_search, q, max(limit, 12))
            f_sug = ex.submit(_yt_suggest, q)
            f_lrc = ex.submit(_lrclib_resolve, q) if want_lyrics else None
            f_ps = ex.submit(_saavn_search, primary, limit) if _compact(primary) != _compact(q) else None
            f_py = ex.submit(_youtube_innertube_search, primary, max(limit, 12)) if _compact(primary) != _compact(q) else None
            saavn_results = f_saavn.result() or []
            yt_results = f_yt.result() or []
            suggestions = f_sug.result() or []
            resolved = (f_lrc.result() or []) if f_lrc else []
            if f_ps:
                saavn_results.extend(f_ps.result() or [])
            if f_py:
                yt_results.extend(f_py.result() or [])
    except Exception as e:
        log.warning(f"Concurrent search executor error: {e}")
        saavn_results = _saavn_search(q, limit)
        yt_results = []

    extras = _extra_search_queries(q, suggestions, resolved)
    if extras:
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                futs = []
                for extra in extras:
                    futs.append(("saavn", ex.submit(_saavn_search, extra, limit)))
                    futs.append(("yt", ex.submit(_youtube_innertube_search, extra, limit)))
                for kind, f in futs:
                    try:
                        got = f.result() or []
                    except Exception:
                        got = []
                    if kind == "saavn":
                        saavn_results.extend(got)
                    else:
                        yt_results.extend(got)
        except Exception as e:
            log.warning(f"Follow-up search error: {e}")

    pinned = list((canon or {}).get("pinned") or [])
    all_items = pinned + saavn_results + yt_results
    if canon:
        originals = [
            x for x in all_items
            if x.get("official") or _canonical_boost(x, q) >= 50_000_000_000 or _has_famous_voice(x)
        ]
        if originals:
            all_items = originals
    all_items.sort(
        key=lambda x: (_canonical_boost(x, q), _unified_score(x, q, resolved)),
        reverse=True,
    )

    # Deduplicate tracks by normalized title while preserving highest-ranking version
    seen = set()
    final = []
    for it in all_items:
        norm_title = _compact(re.sub(r"\(.*?\)|\[.*?\]", "", it.get("title") or ""))[:28]
        if not norm_title or norm_title in seen:
            continue
        seen.add(norm_title)
        final.append({
            "video_id": it["video_id"],
            "title": it["title"],
            "thumbnail": it["thumbnail"],
            "duration": it.get("duration"),
            "channel": it.get("channel", "Music"),
        })
        if len(final) >= limit:
            break

    results = final if final else (saavn_results or yt_results)

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
        "version": "6.0",
        "engine": "official_originals+jiosaavn+youtube",
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
        "version": "6.0",
        "engine": "official_originals+jiosaavn+youtube",
        "yt_dlp_version": ver,
        "saavn_cache_size": len(_SAAVN_MEDIA_CACHE),
        "search_cache_size": len(_SEARCH_CACHE),
        "audio_cache_size": len(_AUDIO_CACHE),
        "git_commit": os.environ.get("RENDER_GIT_COMMIT", ""),
        "git_branch": os.environ.get("RENDER_GIT_BRANCH", ""),
        "service_id": os.environ.get("RENDER_SERVICE_ID", ""),
        "cwd": os.getcwd(),
        "main_file": os.path.abspath(__file__),
    }


@app.get("/search_results")
def search_results(query: str, limit: int = 8):
    if not query or not query.strip():
        raise HTTPException(400, "query required")
    q = query.strip()
    results = _search_metadata(q, min(max(limit, 1), 20))
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

    # 1. Instant search — try the top ranked matches
    results = _search_metadata(q, limit=5)
    for hit in results:
        vid = hit.get("video_id")
        if not vid:
            continue
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
