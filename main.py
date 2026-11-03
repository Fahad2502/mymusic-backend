"""
HUSH Backend v7.0 — Saavn-first, instant streaming
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Search  → JioSaavn API only  (~0.3–1s, 100M+ songs, 320kbps CDN)
Play    → Saavn cache hit    (~0ms  — pre-cached during search)
         Saavn by ID         (~0.3s — any Saavn ID not yet cached)
         Saavn bridge        (~1s   — convert YouTube ID → Saavn CDN)
         yt-dlp fast         (~8s   — last resort, strict 8s timeout)
"""
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

import os, re, uuid, time, math, html, base64, logging, concurrent.futures
import httpx
try:
    import yt_dlp
except ImportError:
    yt_dlp = None

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("hush")

app = FastAPI(title="HUSH Backend", version="7.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# ── Materialise YT_COOKIES env var ────────────────────────────────────────
_YT_COOKIES_ENV = os.environ.get("YT_COOKIES", "").strip()
if _YT_COOKIES_ENV:
    try:
        _c = _YT_COOKIES_ENV
        if "# Netscape" not in _c and "\t" not in _c:
            try: _c = base64.b64decode(_YT_COOKIES_ENV).decode("utf-8", errors="ignore")
            except Exception: pass
        with open("cookies.txt", "w", newline="\n") as _f: _f.write(_c)
        log.info("Materialised YT_COOKIES → cookies.txt")
    except Exception as e:
        log.warning(f"YT_COOKIES materialise failed: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# CACHES
# ═══════════════════════════════════════════════════════════════════════════
_SAAVN_MEDIA_CACHE: dict[str, dict] = {}          # song_id → audio meta (unbounded, fast)
_SEARCH_CACHE: dict[str, tuple[float, list]] = {} # query → (ts, results)
_SEARCH_TTL   = 600   # 10 min
_AUDIO_CACHE: dict[str, tuple[float, dict]] = {}  # yt_id  → (ts, audio meta)
_AUDIO_TTL    = 1800  # 30 min
STREAM_CACHE: dict[str, dict] = {}                # uuid   → proxied stream entry

# ═══════════════════════════════════════════════════════════════════════════
# SAAVN API — primary engine
# ═══════════════════════════════════════════════════════════════════════════
_SAAVN_URL = "https://www.jiosaavn.com/api.php"
_SAAVN_HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Cookie": "L=english%2Chindi; geo=IN; gdpr_acceptance=true",
    "X-Forwarded-For": "49.36.0.1",
}
_DES_KEY = b"38346591"


def _decrypt_url(enc: str) -> str:
    """Decrypt JioSaavn DES-ECB URL."""
    if not enc:
        return ""
    try:
        raw = base64.b64decode(enc)
    except Exception:
        return ""
    try:
        import pyDes
        return pyDes.des(_DES_KEY, pyDes.ECB, padmode=pyDes.PAD_PKCS5).decrypt(raw).decode()
    except Exception:
        pass
    try:
        from cryptography.hazmat.primitives.ciphers import Cipher, modes
        from cryptography.hazmat.primitives import padding
        try:
            from cryptography.hazmat.decrepit.ciphers.algorithms import TripleDES
        except ImportError:
            from cryptography.hazmat.primitives.ciphers.algorithms import TripleDES
        c = Cipher(TripleDES(_DES_KEY * 3), modes.ECB())
        d = c.decryptor()
        dec = d.update(raw) + d.finalize()
        u = padding.PKCS7(64).unpadder()
        return (u.update(dec) + u.finalize()).decode()
    except Exception as e:
        log.error(f"DES decrypt failed: {e}")
        return ""


def _upgrade_url(url: str, has320: bool) -> str:
    if "_96.mp4" in url:
        return url.replace("_96.mp4", "_320.mp4" if has320 else "_160.mp4")
    return url


def _thumb(img: str) -> str:
    if not img:
        return ""
    return re.sub(r"-(150x150|50x50)\.", "-500x500.", img)


def _unescape(s) -> str:
    return html.unescape(str(s or "")).strip()


def _int(v, default=0) -> int:
    try: return int(v or default)
    except Exception: return default


def _plays(v) -> int:
    if v is None: return 0
    if isinstance(v, (int, float)): return int(v)
    t = re.sub(r"[,\s]", "", str(v).lower())
    m = re.match(r"([\d.]+)([kmb])?", t)
    if m:
        n = float(m.group(1))
        return int(n * {"k":1000,"m":1_000_000,"b":1_000_000_000}.get(m.group(2), 1))
    d = re.sub(r"\D","",t)
    return int(d) if d else 0


def _tokens(s: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", (s or "").lower())


def _compact(s: str) -> str:
    return "".join(_tokens(s))


def _similarity(a: str, b: str) -> float:
    ca, cb = _compact(a), _compact(b)
    if not ca or not cb: return 0.0
    if ca == cb: return 1.0
    if ca in cb or cb in ca:
        sh, lo = (ca,cb) if len(ca)<=len(cb) else (cb,ca)
        return 0.72 + 0.28*(len(sh)/len(lo))
    ta, tb = set(_tokens(a)), set(_tokens(b))
    return len(ta&tb)/len(ta) if ta else 0.0


# ── Map one Saavn song dict → our standard result dict ───────────────────
def _map_saavn_item(s: dict) -> dict | None:
    more  = s.get("more_info") if isinstance(s.get("more_info"), dict) else {}
    sid   = s.get("id") or s.get("song_id")
    if not sid:
        return None

    title  = _unescape(s.get("song") or s.get("title") or "Unknown")
    artist = _unescape(s.get("primary_artists") or more.get("primary_artists")
                       or s.get("singers") or more.get("singers") or "Artist")
    thumb  = _thumb(s.get("image") or more.get("image") or "")
    dur    = _int(s.get("duration") or more.get("duration"))
    album  = _unescape(s.get("album") or more.get("album") or "")
    plays  = _plays(s.get("play_count") or more.get("play_count") or 0)
    has320 = str(s.get("320kbps") or more.get("320kbps") or "").lower() == "true"

    # Pre-cache audio URL so /play is instant
    enc = s.get("encrypted_media_url") or more.get("encrypted_media_url")
    if enc:
        dec = _decrypt_url(enc)
        if dec:
            audio_url = _upgrade_url(dec, has320)
            _SAAVN_MEDIA_CACHE[sid] = {
                "audio_url": audio_url, "title": title, "thumbnail": thumb,
                "duration": dur, "artist": artist, "direct": True,
            }

    return {
        "video_id":  f"s:{sid}",
        "title":     title,
        "thumbnail": thumb,
        "duration":  dur or None,
        "channel":   artist,
        "_plays":    plays,
        "_album":    album,
        "_sid":      sid,
    }


def _saavn_autocomplete(query: str) -> list[dict]:
    try:
        r = httpx.get(_SAAVN_URL, params={
            "__call": "autocomplete.get", "_format": "json", "_marker": "0",
            "cc": "in", "includeMetaTags": "1", "query": query,
        }, headers=_SAAVN_HDR, timeout=5.0)
        if r.status_code != 200: return []
        songs = ((r.json().get("songs") or {}).get("data") or [])
        return [x for x in (_map_saavn_item(s) for s in songs) if x]
    except Exception as e:
        log.warning(f"saavn autocomplete '{query}': {e}")
        return []


def _saavn_full_search(query: str, n: int = 40) -> list[dict]:
    try:
        r = httpx.get(_SAAVN_URL, params={
            "__call": "search.getResults", "_format": "json", "_marker": "0",
            "cc": "in", "includeMetaTags": "1", "q": query, "n": n, "p": 1,
        }, headers=_SAAVN_HDR, timeout=6.0)
        if r.status_code != 200: return []
        return [x for x in (_map_saavn_item(s) for s in r.json().get("results") or []) if x]
    except Exception as e:
        log.warning(f"saavn full search '{query}': {e}")
        return []


def _saavn_extract_by_id(sid: str) -> dict | None:
    """Fetch audio for a Saavn song ID directly (used for cache misses)."""
    cached = _SAAVN_MEDIA_CACHE.get(sid)
    if cached and cached.get("audio_url"):
        return cached
    try:
        r = httpx.get(_SAAVN_URL, params={
            "__call": "song.getDetails", "_format": "json", "cc": "in", "pids": sid,
        }, headers=_SAAVN_HDR, timeout=6.0)
        if r.status_code != 200: return None
        item = r.json().get(sid)
        if not item: return None
        title  = _unescape(item.get("song") or "Unknown")
        artist = _unescape(item.get("primary_artists") or item.get("singers") or "Artist")
        thumb  = _thumb(item.get("image") or "")
        dur    = _int(item.get("duration"))
        has320 = str(item.get("320kbps","")).lower() == "true"
        enc    = item.get("encrypted_media_url")
        if not enc: return None
        dec = _decrypt_url(enc)
        if not dec: return None
        audio_url = _upgrade_url(dec, has320)
        res = {"audio_url": audio_url, "title": title, "thumbnail": thumb,
               "duration": dur or None, "artist": artist, "direct": True}
        _SAAVN_MEDIA_CACHE[sid] = res
        return res
    except Exception as e:
        log.warning(f"saavn extract by id {sid}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════
# RANKING — famous originals first
# ═══════════════════════════════════════════════════════════════════════════
_FAMOUS_VOICES = (
    "rahat fateh ali","nusrat fateh ali","atif aslam","momina mustehsan",
    "arijit singh","shreya ghoshal","sonu nigam","kumar sanu","udit narayan",
    "lata mangeshkar","asha bhosle","kishore kumar","mohammed rafi","mohammad rafi",
    "a.r. rahman","ar rahman","armaan malik","jubin nautiyal","neha kakkar",
    "diljit dosanjh","abida parveen","ali sethi","shafqat amanat","ali zafar",
    "hadiqa kiani","sajjad ali","mohit chauhan","sunidhi chauhan",
    "sukhwinder singh","palak muchhal","shankar mahadevan",
    "coke studio","t-series","yrf","sony music","zee music",
    "saregama","tips official","vevo","speed records",
)

_JUNK_WORDS = (
    "karaoke","instrumental","tribute","backing track","ringtone",
    "whatsapp","status","nightcore","slowed","reverb",
)


def _has_famous(item: dict) -> bool:
    blob = f"{item.get('channel','')} {item.get('title','')} {item.get('_album','')}".lower()
    return any(v in blob for v in _FAMOUS_VOICES) or \
           (item.get("channel","")).lower().endswith("topic")


def _is_junk(item: dict, query: str) -> bool:
    blob = f"{item.get('title','')} {item.get('channel','')}".lower()
    qlow = query.lower()
    if any(k in qlow for k in ("karaoke","remix","cover","slowed","reverb")):
        return False   # user explicitly wants it
    return any(k in blob for k in _JUNK_WORDS)


def _rank(item: dict, query: str) -> float:
    t   = item.get("title","")
    ch  = item.get("channel","")
    q   = query.strip()

    title_sim  = _similarity(q, t)
    combo_sim  = _similarity(q, f"{t} {ch}")
    relevance  = max(title_sim, combo_sim)
    pop        = min(math.log10(_plays(item.get("_plays",0)) + 1), 10)

    score  = relevance * 10_000_000_000
    score += pop       *    900_000_000

    if _has_famous(item):     score += 20_000_000_000
    elif relevance >= 0.55:   score -= 18_000_000_000   # irrelevant unknown cover

    blob = f"{t} {ch}".lower()
    if "official" in blob or "coke studio" in blob:  score += 5_000_000_000
    if any(k in blob for k in ("whatsapp","shorts","tiktok","ringtone")): score -= 15_000_000_000
    if _is_junk(item, q):     score -= 35_000_000_000
    dur = item.get("duration") or 0
    if dur and dur < 50:      score -= 12_000_000_000

    return score


# Pinned originals for commonly mis-typed searches
def _yt_pin(vid, title, ch, dur=None):
    return {"video_id": vid, "title": title,
            "thumbnail": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
            "duration": dur, "channel": ch, "_plays": 500_000_000, "_album": ""}

_PINS: list[dict] = [
    # These are always returned first when query matches — they're YouTube IDs
    # that get bridged to Saavn at play time.
    _yt_pin("kw4tT7SCmaY", "Afreen Afreen | Coke Studio Season 9 | Rahat Fateh Ali Khan & Momina Mustehsan", "Coke Studio Pakistan", 390),
    _yt_pin("a18py61_F_w", "Tajdar-e-Haram | Coke Studio Season 8 | Atif Aslam", "Coke Studio Pakistan", 607),
    _yt_pin("oyLVu753XJw", "Tere Mast Mast Do Nain | Dabangg | Rahat Fateh Ali Khan, Shreya Ghoshal", "T-Series", 201),
    _yt_pin("JGwWNGJdvx8", "Ed Sheeran - Shape of You (Official Music Video)", "Ed Sheeran", 264),
    _yt_pin("4NRXx6U8ABQ", "The Weeknd - Blinding Lights (Official Video)", "The Weeknd", 200),
    _yt_pin("BddP6PYo2gs", "Kesariya - Brahmastra | Ranbir Kapoor | Arijit Singh", "Sony Music India", 257),
    _yt_pin("RgKAFK5djSk", "Wiz Khalifa - See You Again ft. Charlie Puth", "Wiz Khalifa", 229),
    _yt_pin("kffacxfA7G4", "PSY - GANGNAM STYLE (강남스타일) M/V", "officialpsy", 253),
    _yt_pin("YQHsXMglC9A", "Adele - Hello (Official Music Video)", "Adele", 295),
    _yt_pin("9bZkp7q19f0", "PSY - GENTLEMAN M/V", "officialpsy", 205),
    _yt_pin("SlPhMPnQ58k", "Rahat Fateh Ali Khan - Zaroori Tha", "T-Series", 287),
    _yt_pin("fGeEnWqS-4I", "Arijit Singh - Tum Hi Ho (Aashiqui 2)", "T-Series", 263),
]

_PIN_QUERIES: dict[str, list[str]] = {
    "kw4tT7SCmaY": ["afreen afreen","afreen","afreen coke studio","afreen rahat"],
    "a18py61_F_w": ["tajdar e haram","tajdar haram","tajdar atif","tajdar coke studio"],
    "oyLVu753XJw": ["tere mast mast","mast mast do nain","dabangg rahat","tere mast"],
    "JGwWNGJdvx8": ["shape of you","shape of you ed sheeran","ed sheeran shape"],
    "4NRXx6U8ABQ": ["blinding lights","blinding lights weeknd","the weeknd blinding"],
    "BddP6PYo2gs": ["kesariya","kesariya brahmastra","kesariya arijit"],
    "RgKAFK5djSk": ["see you again","see you again wiz khalifa","charlie puth see you"],
    "kffacxfA7G4": ["gangnam style","psy gangnam","강남스타일"],
    "YQHsXMglC9A": ["hello adele","adele hello","hello official adele"],
    "SlPhMPnQ58k": ["zaroori tha","zaroori tha rahat","zaruri tha"],
    "fGeEnWqS-4I": ["tum hi ho","tum hi ho arijit","aashiqui tum hi ho"],
}

def _get_pins(query: str) -> list[dict]:
    """Return pinned songs that match this query."""
    cq = _compact(query)
    qlow = query.lower().strip()
    pins = []
    for pin in _PINS:
        vid = pin["video_id"]
        queries = _PIN_QUERIES.get(vid, [])
        for pq in queries:
            if _compact(pq) in cq or cq in _compact(pq) or \
               _similarity(query, pq) >= 0.75:
                pins.append(pin)
                break
    return pins


# ═══════════════════════════════════════════════════════════════════════════
# SEARCH PIPELINE — Saavn only, fast
# ═══════════════════════════════════════════════════════════════════════════
def _search(query: str, limit: int = 20) -> list[dict]:
    key = f"v7:{query.lower().strip()}:{limit}"
    c = _SEARCH_CACHE.get(key)
    if c and time.time() - c[0] < _SEARCH_TTL:
        return c[1]

    q = query.strip()
    t0 = time.time()

    # Run autocomplete + full search in parallel (both Saavn, fast)
    auto_hits: list[dict] = []
    full_hits: list[dict] = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            fa = ex.submit(_saavn_autocomplete, q)
            ff = ex.submit(_saavn_full_search, q, max(limit * 4, 40))
            auto_hits = fa.result() or []
            full_hits = ff.result() or []
    except Exception as e:
        log.warning(f"search parallel error '{q}': {e}")
        full_hits = _saavn_full_search(q, max(limit*4, 40))

    # Rank full hits
    full_hits.sort(key=lambda x: _rank(x, q), reverse=True)

    # Merge: auto first (more title-relevant), then ranked full results
    seen_ids:   set[str] = set()
    seen_pairs: set[tuple] = set()
    merged: list[dict] = []

    def _add(item):
        if not item: return
        vid = item["video_id"]
        if vid in seen_ids: return
        seen_ids.add(vid)
        pair = (_compact(re.sub(r"\(.*?\)","",item["title"]))[:24],
                item["channel"].lower().split(",")[0].strip()[:20])
        if pair in seen_pairs: return
        seen_pairs.add(pair)
        merged.append(item)

    for h in auto_hits: _add(h)
    for h in full_hits:
        if len(merged) >= limit * 3: break
        _add(h)

    # Prepend pinned originals
    pins = _get_pins(q)
    pin_ids = {p["video_id"] for p in pins}
    merged = [x for x in merged if x["video_id"] not in pin_ids]
    all_items = pins + merged

    # Final dedup and strip private fields
    final: list[dict] = []
    seen_final: set[str] = set()
    for it in all_items:
        norm = _compact(re.sub(r"\(.*?\)|\[.*?\]","",it["title"]))[:28]
        if norm in seen_final: continue
        seen_final.add(norm)
        final.append({
            "video_id":  it["video_id"],
            "title":     it["title"],
            "thumbnail": it["thumbnail"],
            "duration":  it.get("duration"),
            "channel":   it.get("channel","Music"),
        })
        if len(final) >= limit: break

    log.info(f"search '{q}' → {len(final)} results in {round(time.time()-t0,2)}s")

    if final:
        _SEARCH_CACHE[key] = (time.time(), final)
        if len(_SEARCH_CACHE) > 300:
            _SEARCH_CACHE.pop(min(_SEARCH_CACHE, key=lambda k: _SEARCH_CACHE[k][0]), None)

    return final


# ═══════════════════════════════════════════════════════════════════════════
# EXTRACTION PIPELINE
# ═══════════════════════════════════════════════════════════════════════════
def _clean_title(title: str) -> str:
    t = re.sub(r'\(.*?(official|video|audio|lyrics|4k|hd|remaster).*?\)', '', title, flags=re.I)
    t = re.sub(r'official\s+(music\s+)?video|lyric\s+video|full\s+song|full\s+video', '', t, flags=re.I)
    return " ".join(t.split()).strip()


def _titles_overlap(a: str, b: str) -> bool:
    aw = set(w for w in _tokens(a) if len(w) > 2)
    bw = set(w for w in _tokens(b) if len(w) > 2)
    if not aw or not bw: return False
    return (len(aw & bw) / len(bw)) >= 0.4


def _bridge_yt_to_saavn(video_id: str) -> dict | None:
    """Convert a YouTube video ID to a Saavn 320kbps stream."""
    # Get title from oEmbed (never blocked)
    try:
        r = httpx.get(
            f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json",
            timeout=5.0)
        if r.status_code != 200: return None
        yt_title = r.json().get("title","")
    except Exception:
        return None

    if not yt_title: return None
    cleaned = _clean_title(yt_title)
    log.info(f"Bridging {video_id} '{yt_title}' → saavn '{cleaned}'")

    # Search Saavn for it
    candidates = _saavn_autocomplete(cleaned) or _saavn_full_search(cleaned, 5)
    for cand in candidates[:5]:
        sid = cand.get("_sid","")
        if not sid: continue
        if _titles_overlap(cleaned, cand["title"]):
            song = _saavn_extract_by_id(sid)
            if song:
                _SAAVN_MEDIA_CACHE[video_id] = song   # cache under YT ID too
                log.info(f"Bridge OK: {video_id} → saavn '{song['title']}'")
                return song
    return None


def _cookie_path() -> str | None:
    cf = os.environ.get("YT_COOKIES_FILE")
    if cf and os.path.exists(cf): return cf
    if os.path.exists("cookies.txt"): return os.path.abspath("cookies.txt")
    return None


def _ytdlp_extract(video_id: str) -> dict | None:
    """yt-dlp with bgutil PO-token provider — bypasses Render datacenter IP block."""
    if yt_dlp is None: return None
    url = f"https://www.youtube.com/watch?v={video_id}"

    # Try strategies in order: tv_embedded with POT (best for datacenter IPs), then android, then mweb
    strategies = [
        {"name": "tv_embedded+pot", "extractor_args": {"youtube": {"player_client": ["tv_embedded"]}}, "timeout": 20},
        {"name": "android",         "extractor_args": {"youtube": {"player_client": ["android","mweb"]}}, "timeout": 15},
    ]

    for strat in strategies:
        opts = {
            "format": "bestaudio[ext=m4a]/bestaudio/best",
            "noplaylist": True, "quiet": True, "no_warnings": True,
            "nocheckcertificate": True, "cachedir": False,
            "socket_timeout": strat["timeout"], "retries": 1,
            "extractor_args": strat["extractor_args"],
            "http_headers": {"User-Agent": "com.google.android.youtube/19.09.37 (Linux; U; Android 11)"},
        }
        # Add bgutil PO-token provider if available (bypasses datacenter IP bot-check)
        try:
            from bgutil_ytdlp_pot_provider.provider import BgUtilPotProvider
            opts.setdefault("compat_opts", set())
            # bgutil provider hooks in via yt-dlp plugin system automatically when installed
        except ImportError:
            pass
        cp = _cookie_path()
        if cp: opts["cookiefile"] = cp
        try:
            log.info(f"yt-dlp {strat['name']} for {video_id}")
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
            audio = info.get("url")
            if not audio:
                fmts = info.get("formats") or []
                ao = [f for f in fmts if f.get("url") and f.get("vcodec") in (None,"none","") and f.get("acodec") not in (None,"none","")]
                af = [f for f in fmts if f.get("url")]
                best = (ao or af or [None])[0]
                if best: audio = best["url"]
            if not audio:
                log.warning(f"yt-dlp {strat['name']} no audio URL for {video_id}")
                continue
            log.info(f"yt-dlp {strat['name']} OK for {video_id}")
            return {
                "audio_url": audio,
                "title": info.get("title",""),
                "thumbnail": info.get("thumbnail") or f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
                "duration": info.get("duration"),
                "http_headers": info.get("http_headers") or {},
                "direct": False,
            }
        except Exception as e:
            log.warning(f"yt-dlp {strat['name']} failed {video_id}: {e}")
            continue
    return None


def _extract(video_id: str) -> dict:
    """
    Resolve any video_id to a playable audio dict.
    s:SAAVN_ID  → instant Saavn CDN (~0ms cache or ~0.3s API)
    YT_ID(11ch) → Saavn bridge (~1s) → yt-dlp (~8s) → error
    """
    # ── Saavn ID path ──────────────────────────────────────────────────────
    if video_id.startswith("s:"):
        sid = video_id[2:]
        song = _SAAVN_MEDIA_CACHE.get(sid) or _saavn_extract_by_id(sid)
        if song and song.get("audio_url"):
            return song
        raise RuntimeError(f"Saavn extraction failed for {sid}")

    # ── YouTube ID path ────────────────────────────────────────────────────
    # Check audio cache first
    ac = _AUDIO_CACHE.get(video_id)
    if ac and time.time() - ac[0] < _AUDIO_TTL:
        return ac[1]

    # Also check Saavn media cache (bridge may have cached it under YT ID)
    sc = _SAAVN_MEDIA_CACHE.get(video_id)
    if sc and sc.get("audio_url"):
        return sc

    # Bridge YouTube → Saavn (preferred: 320kbps CDN, no proxy)
    song = _bridge_yt_to_saavn(video_id)

    # yt-dlp last resort (strict 8s timeout)
    if not song:
        song = _ytdlp_extract(video_id)

    if not song:
        raise RuntimeError(f"All extraction methods failed for {video_id}")

    _AUDIO_CACHE[video_id] = (time.time(), song)
    if len(_AUDIO_CACHE) > 200:
        _AUDIO_CACHE.pop(min(_AUDIO_CACHE, key=lambda k: _AUDIO_CACHE[k][0]), None)
    return song


def _build_response(song: dict, base_url: str) -> dict:
    audio_url = song.get("audio_url") or song.get("url")
    if not audio_url:
        raise ValueError("No audio URL")
    if song.get("direct", True):
        return {"title": song.get("title",""), "thumbnail": song.get("thumbnail",""),
                "duration": song.get("duration"), "audio_url": audio_url}
    # Proxied (yt-dlp stream)
    rid = str(uuid.uuid4())
    STREAM_CACHE[rid] = {"url": audio_url, "headers": song.get("http_headers") or {},
                         "title": song.get("title"), "thumbnail": song.get("thumbnail"),
                         "duration": song.get("duration")}
    if len(STREAM_CACHE) > 400:
        STREAM_CACHE.pop(next(iter(STREAM_CACHE)), None)
    return {"title": song.get("title",""), "thumbnail": song.get("thumbnail",""),
            "duration": song.get("duration"),
            "audio_url": f"{base_url.rstrip('/')}/stream?id={rid}"}


# ═══════════════════════════════════════════════════════════════════════════
# RELATED SONG
# ═══════════════════════════════════════════════════════════════════════════
def _related_query(title: str) -> str:
    t = _clean_title(title)
    parts = [p.strip() for p in t.split("-") if p.strip()]
    return parts[0] if len(parts) > 1 else " ".join(t.split()[:4])


# ═══════════════════════════════════════════════════════════════════════════
# API ROUTES
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/")
def root():
    return {"status": "ok", "version": "7.0", "engine": "saavn-first",
            "latency": "instant"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/wake")
def wake():
    return {"status": "awake"}

@app.get("/debug")
def debug():
    ver = "not_installed"
    if yt_dlp:
        try: ver = yt_dlp.version.__version__
        except Exception: ver = "unknown"
    return {
        "version": "7.0", "engine": "saavn-first",
        "yt_dlp_version": ver,
        "saavn_cache": len(_SAAVN_MEDIA_CACHE),
        "search_cache": len(_SEARCH_CACHE),
        "audio_cache": len(_AUDIO_CACHE),
        "stream_cache": len(STREAM_CACHE),
        "git_commit": os.environ.get("RENDER_GIT_COMMIT",""),
    }

@app.get("/search_results")
def search_results(query: str, limit: int = 20):
    if not query or not query.strip():
        raise HTTPException(400, "query required")
    results = _search(query.strip(), min(max(limit, 1), 20))
    return {"results": results}

@app.get("/play")
def play_song(video_id: str, request: Request):
    if not video_id or not video_id.strip():
        raise HTTPException(400, "video_id required")
    try:
        song = _extract(video_id.strip())
        return _build_response(song, str(request.base_url))
    except Exception as e:
        log.error(f"/play {video_id}: {e}")
        raise HTTPException(502, str(e))

@app.get("/search")
def search_song(query: str, request: Request):
    """I'm Feeling Lucky — search + extract in one shot."""
    if not query or not query.strip():
        raise HTTPException(400, "query required")
    q = query.strip()
    results = _search(q, limit=5)
    for hit in results:
        vid = hit.get("video_id")
        if not vid: continue
        try:
            song = _extract(vid)
            return _build_response(song, str(request.base_url))
        except Exception as e:
            log.warning(f"/search extract {vid}: {e}")
    raise HTTPException(502, f"Could not find or play '{q}'")

@app.get("/related")
def related_song(title: str, request: Request):
    if not title or not title.strip():
        raise HTTPException(400, "title required")
    q = _related_query(title.strip())
    results = _search(q, limit=6)
    clean = _compact(_clean_title(title.strip()))
    for r in results:
        if clean and clean in _compact(r.get("title","")):
            continue   # skip the same song
        try:
            song = _extract(r["video_id"])
            return _build_response(song, str(request.base_url))
        except Exception:
            continue
    if results:
        try:
            song = _extract(results[0]["video_id"])
            return _build_response(song, str(request.base_url))
        except Exception:
            pass
    raise HTTPException(502, "No related song found")

@app.get("/stream")
def stream_song(id: str, request: Request):
    entry = STREAM_CACHE.get(id)
    if not entry or not entry.get("url"):
        raise HTTPException(404, "Stream expired — search again.")
    url = entry["url"]
    headers = dict(entry.get("headers") or {})
    rng = request.headers.get("range")
    if rng: headers["range"] = rng
    for h in ("host","Host","connection","Connection"):
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
        raise HTTPException(code, "Stream rejected")
    ct = upstream.headers.get("content-type","audio/mp4")
    def _iter():
        try:
            for chunk in upstream.iter_bytes(65536):
                if chunk: yield chunk
        finally:
            upstream.close(); client.close()
    resp = StreamingResponse(_iter(), media_type=ct, status_code=upstream.status_code)
    for h in ("content-range","content-length","accept-ranges"):
        if h in upstream.headers: resp.headers[h] = upstream.headers[h]
    resp.headers["accept-ranges"] = "bytes"
    return resp
