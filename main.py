"""
TikTok Downloader API · fuente única: ttdownloader

Recibe un link de TikTok y devuelve el .mp4 directamente a quien lo pidió.
No recodifica nada: entrega el archivo tal cual lo sirve ttdownloader.
API pública: cualquiera que conozca el dominio puede usarla (sin API key).

Variables de entorno (todas opcionales):
  API_KEY          si la defines, los clientes deben mandarla en el header X-API-Key (o ?key=).
                   Vacía o sin definir = API pública (por defecto).
  MAX_CONCURRENT   descargas simultáneas máximas (default 2)
  ATTEMPTS         reintentos con ttdownloader por cada link (default 3)
  TOTAL_TIMEOUT    segundos máximos por petición, contando reintentos (default 90)
  ALLOWED_ORIGINS  orígenes CORS separados por coma (default *)
"""
import os
import re
import json
import time
import types
import shutil
import secrets
import logging
import tempfile
import socket
import threading
import ipaddress
import subprocess
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout
from urllib.parse import urlparse

import httpx
import requests
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from starlette.background import BackgroundTask

try:  # opcional: ayuda si tikwm.com muestra un reto de Cloudflare
    import cloudscraper
except Exception:
    cloudscraper = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tiktok-api")

# ───────── Configuración ─────────
API_KEY = os.getenv("API_KEY", "").strip()  # vacío = pública
MAX_CONCURRENT = max(1, int(os.getenv("MAX_CONCURRENT", "2")))
ATTEMPTS = max(1, int(os.getenv("ATTEMPTS", "3")))
TOTAL_TIMEOUT = int(os.getenv("TOTAL_TIMEOUT", "90"))
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]

SOURCE_NAME = "ttdownloader"
MIN_BYTES = 10_000  # por debajo de esto no puede ser un video real
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"}
EXPOSED = ["X-Source", "X-Resolution", "X-FPS", "X-Bitrate-Kbps", "X-Codec", "X-Duration", "X-Watermark", "X-Video-Id"]


# ───────── Fuente: solo ttdownloader ─────────
def _load_source():
    try:
        import tiktok_downloader as td
        fn = getattr(td, "ttdownloader", None)
        if isinstance(fn, types.ModuleType):  # por si el paquete expone el módulo y no la función
            fn = getattr(fn, "ttdownloader", None)
        if not callable(fn):
            raise ImportError("tiktok_downloader no expone 'ttdownloader'")
        return fn, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


ttdownloader, IMPORT_ERROR = _load_source()
if IMPORT_ERROR:
    log.error("⚠️ No pude cargar ttdownloader: %s", IMPORT_ERROR)
else:
    log.info("✓ ttdownloader cargado · API %s", "pública" if not API_KEY else "protegida con API key")


# ───────── Utilidades ─────────
def stream_to(link, path):
    """Guarda el archivo tal cual lo sirve el enlace (sin recodificar)."""
    headers = dict(UA, Referer="https://ttdownloader.com/")
    timeout = httpx.Timeout(30.0, connect=10.0)
    with httpx.stream("GET", link, headers=headers, follow_redirects=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_bytes(1 << 16):
                f.write(chunk)


def video_id(url):
    m = re.search(r"/(?:video|photo)/(\d+)", url)
    if m:
        return m.group(1)
    return str(int(time.time()))  # links cortos: no se pide nada a la red (más rápido)


def _looks_like_mp4(path):
    try:
        with open(path, "rb") as f:
            head = f.read(12)
        return len(head) >= 8 and head[4:8] == b"ftyp"
    except OSError:
        return False


def probe(path):
    """Lee resolución/fps/bitrate con ffprobe. 'ok' = el archivo realmente tiene video."""
    size = os.path.getsize(path)
    info = dict(ok=False, w=0, h=0, codec="?", fps=0, size=size, kbps=0, dur=0)
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=codec_name,width,height,r_frame_rate:format=duration",
             "-of", "json", path],
            capture_output=True, text=True, check=True, timeout=30).stdout
        j = json.loads(out)
    except FileNotFoundError:  # sin ffprobe (solo pasa fuera de Docker): valida por la firma del mp4
        info["ok"] = _looks_like_mp4(path)
        return info
    except Exception:
        return info

    streams = j.get("streams") or []
    if not streams:  # p. ej. si ttdownloader devolvió el audio (.mp3) en vez del video
        return info
    s = streams[0]
    try:
        dur = float((j.get("format") or {}).get("duration") or 0)
    except ValueError:
        dur = 0
    try:
        a, b = s["r_frame_rate"].split("/")
        fps = round(int(a) / int(b), 2) if int(b) else 0
    except Exception:
        fps = 0
    info.update(ok=True, w=int(s.get("width") or 0), h=int(s.get("height") or 0),
                codec=s.get("codec_name", "?"), fps=fps,
                kbps=int(size * 8 / dur / 1000) if dur else 0, dur=round(dur, 1))
    return info


def is_tiktok_url(url):
    """Solo acepta links de TikTok (evita que usen la API para otras webs)."""
    try:
        u = urlparse(url)
        h = (u.hostname or "").lower()
        return u.scheme in ("http", "https") and (h == "tiktok.com" or h.endswith(".tiktok.com"))
    except Exception:
        return False


def _remove(path):
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


# ───────── Descarga con ttdownloader ─────────
def _download(url, path):
    """Un intento: pide los enlaces a ttdownloader y se queda con el primer video válido."""
    result = ttdownloader(url)
    try:
        items = list(result)
    except TypeError:
        items = [result]
    if not items:
        raise RuntimeError("ttdownloader no devolvió enlaces")

    # sin marca de agua primero (sort estable: si la librería no informa, se respeta su orden)
    items.sort(key=lambda i: getattr(i, "watermark", None) is True)

    last = RuntimeError("ttdownloader no devolvió ningún video válido")
    for it in items:
        link = getattr(it, "url", None)
        for mode in ("lib", "direct"):  # 1) descarga de la librería, 2) descarga directa del enlace
            try:
                _remove(path)
                if mode == "lib":
                    it.download(path)
                elif link:
                    stream_to(link, path)
                else:
                    continue
                if not (os.path.exists(path) and os.path.getsize(path) >= MIN_BYTES):
                    raise RuntimeError("archivo vacío")
                info = probe(path)
                if not info["ok"]:
                    raise RuntimeError("el archivo descargado no es un video")
                return dict(info, wm=getattr(it, "watermark", None))
            except Exception as e:
                last = e
    _remove(path)
    raise last


def run_source(url, workdir, base):
    path = os.path.join(workdir, f"{base}__{SOURCE_NAME}.mp4")
    try:
        c = _download(url, path)
        c.update(name=SOURCE_NAME, path=path)
        log.info("✓ %s: %sx%s · %.1f MB · marca=%s", SOURCE_NAME, c["w"], c["h"], c["size"] / 1e6,
                 {True: "sí", False: "no", None: "?"}[c["wm"]])
        return c
    except Exception as e:
        log.info("✗ %s: %s", SOURCE_NAME, str(e)[:120])
        _remove(path)
        return None


def fetch_best(url, workdir):
    """Pide el video a ttdownloader con reintentos y un tiempo máximo total."""
    if ttdownloader is None:
        raise RuntimeError(f"ttdownloader no está disponible: {IMPORT_ERROR}")

    base = f"tiktok_{video_id(url)}"
    deadline = time.monotonic() + TOTAL_TIMEOUT
    pool = ThreadPoolExecutor(max_workers=1)  # así un intento colgado no bloquea la petición
    try:
        for attempt in range(1, ATTEMPTS + 1):
            left = deadline - time.monotonic()
            if left <= 3:
                break
            fut = pool.submit(run_source, url, workdir, base)
            try:
                c = fut.result(timeout=left)
            except FutTimeout:
                log.warning("ttdownloader no respondió a tiempo")
                break
            if c:
                return _finalize(c, workdir, base)
            if attempt < ATTEMPTS:
                log.info("reintento %d/%d", attempt + 1, ATTEMPTS)
                time.sleep(min(0.5 * attempt, max(0.0, deadline - time.monotonic() - 3)))
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    return None


def _finalize(best, workdir, base):
    final = os.path.join(workdir, f"{base}.mp4")
    os.replace(best["path"], final)
    best["path"] = final
    best["base"] = base
    return best


# ───────── Analytics: estadísticas + calidad real (une varias fuentes) ─────────
#   1) tikwm.com        -> vistas, likes, comentarios, favoritos, compartidos, descargas, región, fecha, sonido, enlaces
#   2) página de TikTok -> escalera de calidades (nombre, códec, bitrate, peso) y hashtags exactos
#   3) ffprobe          -> resolución, fps, códec y bitrate REALES de cada versión (se mide, no se adivina)
TIKWM = "https://www.tikwm.com"
CACHE_TTL = 300
_cache = {}
_tikwm_lock = threading.Lock()
_tikwm_last = [0.0]


def _int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _first(*vals):
    for v in vals:
        if v is not None:
            return v
    return None


def _abs(u):
    if not u:
        return None
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return TIKWM + u
    return u


def _codec(name):
    n = str(name or "").lower()
    if "265" in n or "hvc" in n or "hev" in n:
        return "hevc"
    if "264" in n or "avc" in n:
        return "h264"
    if "av1" in n or "av01" in n:
        return "av1"
    if "vp9" in n:
        return "vp9"
    return n


def _fps(v):
    try:
        a, b = str(v).split("/")
        f = round(int(a) / int(b), 2) if int(b) else 0
        return f or None
    except Exception:
        return None


def _short_side(w, h, gear=""):
    if w and h:
        return min(w, h)
    m = re.search(r"_(\d{3,4})_", gear or "")
    return int(m.group(1)) if m else None


def _public_host(url):
    """Evita que ffprobe sea apuntado a direcciones internas (solo hosts públicos)."""
    try:
        u = urlparse(url)
        if u.scheme not in ("http", "https") or not u.hostname:
            return False
        port = u.port or (443 if u.scheme == "https" else 80)
        for info in socket.getaddrinfo(u.hostname, port):
            if not ipaddress.ip_address(info[4][0].split("%")[0]).is_global:
                return False
        return True
    except Exception:
        return False


def probe_remote(url):
    """Mide una versión del video leyendo solo su cabecera por HTTP (rápido)."""
    if not url or not _public_host(url):
        return None
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-rw_timeout", "12000000", "-user_agent", UA["User-Agent"],
             "-protocol_whitelist", "http,https,tcp,tls,crypto", "-select_streams", "v:0",
             "-show_entries", "stream=codec_name,width,height,avg_frame_rate,r_frame_rate,bit_rate:format=duration,size,bit_rate",
             "-of", "json", url],
            capture_output=True, text=True, check=True, timeout=25).stdout
        j = json.loads(out)
    except Exception:
        return None
    streams = j.get("streams") or []
    if not streams:
        return None
    s, fmt = streams[0], j.get("format") or {}
    size = _int(fmt.get("size"))
    try:
        dur = float(fmt.get("duration") or 0)
    except ValueError:
        dur = 0
    br = _first(_int(s.get("bit_rate")), _int(fmt.get("bit_rate")), int(size * 8 / dur) if size and dur else None)
    return dict(width=_int(s.get("width")), height=_int(s.get("height")),
                fps=_fps(s.get("avg_frame_rate")) or _fps(s.get("r_frame_rate")),
                codec=_codec(s.get("codec_name")), bitrate=br, size=size)


def _clients():
    yield requests
    if cloudscraper:
        yield cloudscraper.create_scraper()


def _tikwm_call(url):
    with _tikwm_lock:  # el plan gratis de tikwm permite 1 petición por segundo
        wait = 1.1 - (time.monotonic() - _tikwm_last[0])
        if wait > 0:
            time.sleep(wait)
        _tikwm_last[0] = time.monotonic()
    headers = dict(UA, Referer=TIKWM + "/", Origin=TIKWM, Cookie="current_language=en")
    form = {"url": url, "count": 12, "cursor": 0, "web": 1, "hd": 1}
    for client in _clients():
        try:
            j = client.post(TIKWM + "/api/", data=form, headers=headers, timeout=15).json()
        except Exception:
            continue
        if isinstance(j, dict):
            if j.get("code") == 0 and isinstance(j.get("data"), dict):
                return j["data"]
            if "limit" in str(j.get("msg", "")).lower():
                return "LIMIT"
    return None


def fetch_tikwm(url):
    for _ in range(2):
        r = _tikwm_call(url)
        if r == "LIMIT":
            time.sleep(1.3)
            continue
        return r
    return None


def fetch_tiktok_page(vid):
    """Lee los datos que TikTok incrusta en la página del video (si no los bloquea)."""
    if not vid or not str(vid).isdigit():
        return None
    try:
        r = requests.get(f"https://www.tiktok.com/@i/video/{vid}", headers=dict(UA, **{"Accept-Language": "en-US,en;q=0.9"}), timeout=12)
        m = re.search(r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>', r.text, re.S)
        if not m:
            return None
        item = json.loads(m.group(1))["__DEFAULT_SCOPE__"]["webapp.video-detail"]["itemInfo"]["itemStruct"]
        return item if isinstance(item, dict) else None
    except Exception:
        return None


def _id_from(url):
    m = re.search(r"/(?:video|photo)/(\d+)", url)
    if m:
        return m.group(1)
    try:  # links cortos: seguir la redirección
        r = requests.get(url, headers=UA, allow_redirects=True, timeout=8)
        m = re.search(r"/(?:video|photo)/(\d+)", r.url)
        return m.group(1) if m else None
    except Exception:
        return None


def hashtags_of(caption, item):
    tags = [c.get("title") for c in ((item or {}).get("challenges") or []) if isinstance(c, dict)]
    tags = [t for t in tags if t] or re.findall(r"#(\w+)", caption or "")
    out, seen = [], set()
    for t in tags:
        if t[0].isdigit() or t.lower() in seen:  # "#120fps" y repetidos no son categorías
            continue
        seen.add(t.lower())
        out.append(t)
    return out[:10]


def merge_analytics(tw, page, probes):
    tw, page = tw or {}, page or {}
    pst, pvid = page.get("stats") or {}, page.get("video") or {}
    au, pau = tw.get("author") or {}, page.get("author") or {}
    mu, pmu = tw.get("music_info") or {}, page.get("music") or {}
    caption = _first(tw.get("title"), page.get("desc")) or ""

    stats = {
        "views": _first(_int(tw.get("play_count")), _int(pst.get("playCount"))),
        "likes": _first(_int(tw.get("digg_count")), _int(pst.get("diggCount"))),
        "comments": _first(_int(tw.get("comment_count")), _int(pst.get("commentCount"))),
        "favorites": _first(_int(tw.get("collect_count")), _int(pst.get("collectCount"))),
        "shares": _first(_int(tw.get("share_count")), _int(pst.get("shareCount"))),
        "downloads": _int(tw.get("download_count")),
    }

    renditions, seen_urls = [], set()
    for label, key, size_key in (("Browser", "play", "size"), ("Phone", "hdplay", "hd_size")):
        u = _abs(tw.get(key))
        if not u or u in seen_urls:
            continue
        seen_urls.add(u)
        p = (probes or {}).get(key) or {}
        renditions.append({
            "label": label, "width": p.get("width"), "height": p.get("height"), "fps": p.get("fps"),
            "bitrate": p.get("bitrate"), "codec": p.get("codec"),
            "size": _first(p.get("size"), _int(tw.get(size_key)) or None)})

    ladder = []
    for b in pvid.get("bitrateInfo") or []:
        pa = b.get("PlayAddr") or {}
        ladder.append({"gear": b.get("GearName") or "", "width": _int(pa.get("Width")) or None,
                       "height": _int(pa.get("Height")) or None, "bitrate": _int(b.get("Bitrate")) or None,
                       "codec": _codec(b.get("CodecType")), "size": _int(pa.get("DataSize")) or None})
    ladder.sort(key=lambda x: (_short_side(x["width"], x["height"], x["gear"]) or 0, x["bitrate"] or 0), reverse=True)

    # "Original" = la versión más grande que TikTok ofrece (TikTok no entrega el archivo subido por el creador)
    cands = [c for c in renditions + ladder if c.get("width") and c.get("height")]
    best = max(cands, key=lambda c: (c["width"] * c["height"], c.get("size") or 0), default=None)
    if not best and pvid.get("width") and pvid.get("height"):
        best = {"width": _int(pvid["width"]), "height": _int(pvid["height"]), "size": None}
    best = {"width": best["width"], "height": best["height"], "size": best.get("size")} if best else None

    compression = None
    deliv = renditions[0] if renditions else None
    if deliv and best and deliv.get("size") and best.get("size"):
        pct = int(round(100 * (1 - deliv["size"] / best["size"])))
        compressed = pct >= 2
        compression = {"percent": max(pct, 0), "compressed": compressed, "delivered": deliv["label"],
                       "delivered_p": _short_side(deliv.get("width"), deliv.get("height")),
                       "best_p": _short_side(best["width"], best["height"])}

    return {
        "id": str(_first(tw.get("id"), page.get("id")) or ""),
        "author": {"username": au.get("unique_id") or pau.get("uniqueId") or "", "nickname": au.get("nickname") or pau.get("nickname") or ""},
        "created": _first(_int(tw.get("create_time")), _int(page.get("createTime"))),
        "caption": caption,
        "hashtags": hashtags_of(caption, page),
        "sound": {"title": mu.get("title") or pmu.get("title") or "", "author": mu.get("author") or pmu.get("authorName") or "",
                  "duration": _first(_int(mu.get("duration")), _int(pmu.get("duration"))),
                  "original": bool(_first(mu.get("original"), pmu.get("original")))},
        "stats": stats,
        "region": tw.get("region") or page.get("locationCreated") or "",
        "duration": _first(_int(tw.get("duration")), _int(pvid.get("duration"))),
        "cover": _abs(tw.get("cover")) or pvid.get("cover") or "",
        "renditions": renditions, "ladder": ladder[:6], "best": best, "compression": compression,
    }


def build_analytics(url):
    hit = _cache.get(url)
    if hit and time.time() - hit[0] < CACHE_TTL:
        return hit[1]
    tw = fetch_tikwm(url)
    vid = str(tw["id"]) if tw and tw.get("id") else _id_from(url)
    play = _abs(tw.get("play")) if tw else None
    hd = _abs(tw.get("hdplay")) if tw else None
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_page = ex.submit(fetch_tiktok_page, vid)
        f_play = ex.submit(probe_remote, play) if play else None
        f_hd = ex.submit(probe_remote, hd) if hd and hd != play else None

        def got(f):
            try:
                return f.result(timeout=30) if f else None
            except Exception:
                return None
        page, probes = got(f_page), {"play": got(f_play), "hdplay": got(f_hd)}
    if not tw and not page:
        return None
    data = merge_analytics(tw, page, probes)
    data["id"] = data["id"] or vid or ""
    if len(_cache) > 200:
        _cache.clear()
    _cache[url] = (time.time(), data)
    return data


# ───────── API ─────────
app = FastAPI(title="TikTok Downloader API", version="2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "X-API-Key"],
    expose_headers=EXPOSED,
)
gate = threading.BoundedSemaphore(MAX_CONCURRENT)


class DownloadBody(BaseModel):
    url: str


def _auth(x_api_key, key):
    if not API_KEY:  # pública: no se pide nada
        return
    given = (x_api_key or key or "").encode()
    if not secrets.compare_digest(given, API_KEY.encode()):
        raise HTTPException(401, "API key inválida")


def _serve(url: str):
    url = url.strip()
    if not is_tiktok_url(url):
        raise HTTPException(400, "Manda un link válido de TikTok")
    if ttdownloader is None:
        raise HTTPException(503, "El descargador no está disponible en el servidor")
    if not gate.acquire(timeout=30):
        raise HTTPException(503, "Servidor ocupado, intenta de nuevo en unos segundos")

    workdir = tempfile.mkdtemp(prefix="tt_")
    try:
        best = fetch_best(url, workdir)
    except Exception:
        shutil.rmtree(workdir, ignore_errors=True)
        log.exception("fallo inesperado descargando %s", url)
        raise HTTPException(500, "Error interno al descargar el video")
    finally:
        gate.release()

    if not best:
        shutil.rmtree(workdir, ignore_errors=True)
        raise HTTPException(502, "ttdownloader no pudo con ese link. Revisa que el video sea público e inténtalo otra vez.")

    wm = best["wm"]
    headers = {
        "X-Source": best["name"],
        "X-Resolution": f"{best['w']}x{best['h']}" if best["w"] and best["h"] else "unknown",
        "X-FPS": str(best["fps"]),
        "X-Bitrate-Kbps": str(best["kbps"]),
        "X-Codec": str(best["codec"]),
        "X-Duration": str(best["dur"]),
        "X-Watermark": "unknown" if wm is None else ("yes" if wm else "no"),
        "X-Video-Id": best["base"].replace("tiktok_", ""),
    }
    return FileResponse(
        best["path"],
        media_type="video/mp4",
        filename=f"{best['base']}.mp4",
        headers=headers,
        background=BackgroundTask(shutil.rmtree, workdir, True),  # borra el temporal al terminar de enviar
    )


@app.get("/")
def home():
    return {
        "status": "ok" if ttdownloader else "error",
        "uso": {
            "GET": "/download?url=<link de TikTok>",
            "POST": '/download con JSON {"url": "<link de TikTok>"}',
            "auth": "pública, no necesita API key" if not API_KEY else "header X-API-Key (o ?key=)",
            "analytics": "GET /analytics?url=<link de TikTok> -> JSON con estadísticas, calidad medida y versiones",
            "respuesta": "archivo .mp4 + headers X-Source, X-Resolution, X-FPS, X-Bitrate-Kbps, X-Codec, X-Duration, X-Watermark",
        },
        "fuentes": [SOURCE_NAME],
        "docs": "/docs",
    }


@app.get("/health")
def health():
    if ttdownloader is None:
        return JSONResponse({"status": "error", "detail": IMPORT_ERROR}, status_code=503)
    return {"status": "ok", "source": SOURCE_NAME}


@app.get("/download")
def download_get(
    url: str = Query(..., description="Link del video de TikTok"),
    x_api_key: str | None = Header(default=None),
    key: str | None = Query(default=None),
):
    _auth(x_api_key, key)
    return _serve(url)


@app.post("/download")
def download_post(
    body: DownloadBody,
    x_api_key: str | None = Header(default=None),
    key: str | None = Query(default=None),
):
    _auth(x_api_key, key)
    return _serve(body.url)


@app.get("/analytics")
def analytics_get(
    url: str = Query(..., description="Link del video de TikTok"),
    x_api_key: str | None = Header(default=None),
    key: str | None = Query(default=None),
):
    _auth(x_api_key, key)
    url = url.strip()
    if not is_tiktok_url(url):
        raise HTTPException(400, "Manda un link válido de TikTok")
    if not gate.acquire(timeout=30):
        raise HTTPException(503, "Servidor ocupado, intenta de nuevo en unos segundos")
    try:
        data = build_analytics(url)
    except Exception:
        log.exception("fallo inesperado en analytics %s", url)
        raise HTTPException(500, "Error interno al analizar el video")
    finally:
        gate.release()
    if not data:
        raise HTTPException(502, "No se pudieron leer los datos de ese video (revisa que sea público).")
    return data
