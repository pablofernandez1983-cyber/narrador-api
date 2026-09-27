"""
narrador-api — Flask backend para Railway
Genera texto con Claude y audio con Google TTS Chirp 3 HD.
Persiste jobs en Supabase, guarda MP3 en Tigris S3.
"""

import os, re, uuid, base64, threading, traceback
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import requests

# ─── Config ───────────────────────────────────────────────────────
DATABASE_URL      = os.environ.get("DATABASE_URL", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_TTS_KEY    = os.environ.get("GOOGLE_TTS_API_KEY", "")
NARRADOR_API_KEY  = os.environ.get("NARRADOR_API_KEY", "")
GEMINI_API_KEY    = os.environ.get("GEMINI_API_KEY", "")

S3_BUCKET   = os.environ.get("AWS_S3_BUCKET_NAME", "")
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "")
S3_REGION   = os.environ.get("AWS_DEFAULT_REGION", "auto")
S3_KEY_ID   = os.environ.get("AWS_ACCESS_KEY_ID", "")
S3_SECRET   = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
S3_PREFIX   = "narrador/"

app = Flask(__name__)
CORS(app)

# ─── Auth ─────────────────────────────────────────────────────────
# Clave de solo lectura para los revisores de Google Play (la app Android pide clave al abrir).
# Con ésta se puede navegar y escuchar, pero no crear, modificar ni borrar nada.
NARRADOR_READONLY_KEY = os.environ.get("NARRADOR_READONLY_KEY", "")

def _check_auth():
    if not NARRADOR_API_KEY:
        return None
    key = request.headers.get("X-API-Key", "")
    if key == NARRADOR_API_KEY:
        return None
    if NARRADOR_READONLY_KEY and key == NARRADOR_READONLY_KEY:
        if request.method in ("GET", "HEAD", "OPTIONS"):
            return None
        return jsonify({"error": "clave de solo lectura"}), 403
    return jsonify({"error": "unauthorized"}), 401

# ─── DB helpers ───────────────────────────────────────────────────
def _conn():
    import psycopg2
    from psycopg2.extras import RealDictCursor
    c = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    return c, RealDictCursor

def _job_from_row(r):
    if not r:
        return None
    d = dict(r)
    d["id"] = str(d["id"])
    if d.get("created_at"):
        d["created_at"] = d["created_at"].isoformat()
    if d.get("updated_at"):
        d["updated_at"] = d["updated_at"].isoformat()
    if d.get("played_at"):
        d["played_at"] = d["played_at"].isoformat()
    d["speed"] = float(d.get("speed") or 1.0)
    return d

def _ensure_schema():
    """Columnas/tabla de la biblioteca (carpetas + progreso de escucha) que usa la app Android.
    Idempotente: corre al arrancar el proceso."""
    c, _ = _conn()
    try:
        with c.cursor() as cur:
            cur.execute("""
                ALTER TABLE narrador_jobs
                  ADD COLUMN IF NOT EXISTS folder      TEXT,
                  ADD COLUMN IF NOT EXISTS position_ms BIGINT  NOT NULL DEFAULT 0,
                  ADD COLUMN IF NOT EXISTS duration_ms BIGINT,
                  ADD COLUMN IF NOT EXISTS listened    BOOLEAN NOT NULL DEFAULT false,
                  ADD COLUMN IF NOT EXISTS played_at   TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS deleted_at  TIMESTAMPTZ,
                  ADD COLUMN IF NOT EXISTS cover_key   TEXT,
                  ADD COLUMN IF NOT EXISTS cover_source TEXT;
                CREATE TABLE IF NOT EXISTS narrador_folders (
                  name       TEXT        PRIMARY KEY,
                  position   INT         NOT NULL DEFAULT 0,
                  sort       TEXT        NOT NULL DEFAULT 'desc',
                  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
            """)
        c.commit()
    finally:
        c.close()

if DATABASE_URL:
    try:
        _ensure_schema()
    except Exception:
        traceback.print_exc()

def _job_create(data):
    jid = str(uuid.uuid4())
    c, RDC = _conn()
    try:
        with c.cursor() as cur:
            cur.execute("""
                INSERT INTO narrador_jobs
                  (id, prompt, model, voice, speed, web_search, status, progress_pct, search_count, folder, title)
                VALUES (%s, %s, %s, %s, %s, %s, 'pending', 0, 0, %s, %s)
            """, (jid, data["prompt"], data["model"], data["voice"],
                  data["speed"], data["web_search"], data.get("folder"), data.get("title")))
        c.commit()
    finally:
        c.close()
    return jid

def _job_get(jid):
    c, RDC = _conn()
    try:
        with c.cursor(cursor_factory=RDC) as cur:
            cur.execute("SELECT * FROM narrador_jobs WHERE id = %s", (jid,))
            return _job_from_row(cur.fetchone())
    finally:
        c.close()

def _job_list(limit=50):
    c, RDC = _conn()
    try:
        with c.cursor(cursor_factory=RDC) as cur:
            cur.execute("SELECT * FROM narrador_jobs WHERE deleted_at IS NULL ORDER BY created_at DESC LIMIT %s", (limit,))
            return [_job_from_row(r) for r in cur.fetchall()]
    finally:
        c.close()

def _job_update(jid, **fields):
    fields["updated_at"] = datetime.now(timezone.utc)
    cols = ", ".join(f"{k} = %s" for k in fields)
    vals = list(fields.values()) + [jid]
    c, _ = _conn()
    try:
        with c.cursor() as cur:
            cur.execute(f"UPDATE narrador_jobs SET {cols} WHERE id = %s", vals)
        c.commit()
    finally:
        c.close()

def _job_delete(jid):
    c, _ = _conn()
    try:
        with c.cursor() as cur:
            cur.execute("DELETE FROM narrador_jobs WHERE id = %s", (jid,))
        c.commit()
    finally:
        c.close()

# ─── S3 / Tigris ──────────────────────────────────────────────────
def _s3():
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3", endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_KEY_ID, aws_secret_access_key=S3_SECRET,
        region_name=S3_REGION, config=Config(signature_version="s3v4"),
    )

def _s3_put(key, body, content_type="audio/mpeg"):
    _s3().put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType=content_type)

def _s3_get(key):
    return _s3().get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()

def _s3_get_range(key, rng):
    o = _s3().get_object(Bucket=S3_BUCKET, Key=key, Range=rng)
    return o["Body"].read(), o.get("ContentRange")

def _s3_delete(key):
    _s3().delete_object(Bucket=S3_BUCKET, Key=key)

# ─── Text utils ───────────────────────────────────────────────────
DURATION_RE = re.compile(
    r'(\d+(?:[.,]\d+)?)\s*(h|hs|hora|horas|min|mins|minuto|minutos)\b', re.I
)

def _word_target(prompt):
    m = DURATION_RE.search(prompt)
    if not m:
        return 30 * 155  # default 30 minutos
    n = float(m.group(1).replace(",", "."))
    minutes = n * 60 if m.group(2).lower().startswith("h") else n
    return round(minutes * 155)

def _clean_markdown(t):
    t = re.sub(r'^#{1,6}\s+', '', t, flags=re.M)
    t = re.sub(r'^---+$', '', t, flags=re.M)
    t = re.sub(r'^\*{3,}$', '', t, flags=re.M)
    t = re.sub(r'\*\*(.+?)\*\*', r'\1', t)
    t = re.sub(r'\*(.+?)\*', r'\1', t)
    t = re.sub(r'^>\s+', '', t, flags=re.M)
    t = re.sub(r'^[-*+]\s+', '', t, flags=re.M)
    t = re.sub(r'^\d+\.\s+', '', t, flags=re.M)
    t = re.sub(r'`{1,3}[^`]*`{1,3}', '', t)
    t = re.sub(r'\[(.+?)\]\(.+?\)', r'\1', t)
    t = re.sub(r'\n{3,}', '\n\n', t)
    return t.strip()

def _split_text(text, max_bytes=3800):
    chunks, current = [], ''
    for p in re.split(r'\n\n+', text):
        cand = f"{current}\n\n{p}" if current else p
        if len(cand.encode('utf-8')) > max_bytes:
            if current:
                chunks.append(current.strip())
            current = p
        else:
            current = cand
    if current:
        chunks.append(current.strip())
    return chunks

SYSTEM_PROMPT = (
    "Sos un guionista que escribe contenido para ser narrado en audio en español rioplatense. "
    "Respondé SIEMPRE con texto limpio listo para leer en voz alta: sin títulos, sin encabezados, "
    "sin markdown, sin listas con viñetas, sin meta-comentarios, sin URLs, sin citas tipo '[1]'. "
    "IMPORTANTE: empezá SIEMPRE directo con el contenido del tema. "
    "Nunca confirmes el pedido, nunca saludes, nunca digas cuánto va a durar — la primera palabra tiene que ser del tema. "
    "Solo párrafos naturales separados por líneas en blanco, con tono conversacional y fluido. "
    "REGLA DE LARGO: el texto se graba a 155 palabras por minuto. "
    "Debés alcanzar el largo pedido EXACTAMENTE — quedarse corto es un error grave. "
    "Si el tema se agota, desarrollá ejemplos, anécdotas, contexto histórico y reflexiones."
)

# ─── Job processor ────────────────────────────────────────────────
def _generate_text(job):
    from anthropic import Anthropic
    client = Anthropic(api_key=ANTHROPIC_API_KEY)

    word_target = _word_target(job["prompt"])
    minutes = max(1, round(word_target / 155))

    # Pedido inicial: además del tema, le damos a Claude el largo EXPLÍCITO en palabras
    # (antes sólo veía "de X minutos" y tenía que deducir el conteo, y se quedaba corto).
    ask = (job["prompt"]
           if DURATION_RE.search(job["prompt"])
           else job["prompt"].rstrip(".! ") + ", de 30 minutos.")
    ask += (f"\n\nOBJETIVO DE LARGO: el guión debe tener al menos {word_target} palabras "
            f"(~{minutes} minutos narrados a 155 palabras por minuto). No concluyas ni te "
            f"despidas antes de alcanzar ese largo.")

    messages = [
        {"role": "user", "content": "Hacé un podcast de 5 minutos sobre los gatos domésticos."},
        {"role": "assistant", "content": (
            "Los gatos llevan miles de años conviviendo con el ser humano, "
            "y aun así siguen siendo una de las criaturas más misteriosas del planeta.")},
        {"role": "user", "content": ask},
    ]

    model = job["model"]
    haiku = model.startswith("claude-haiku")
    # Los modelos nuevos piensan antes de escribir y eso cuenta en max_tokens: más margen.
    base_kwargs = dict(model=model, max_tokens=32000 if haiku else 64000, system=SYSTEM_PROMPT)
    if job["web_search"]:
        # web_search_20260209 (filtrado dinámico) en Sonnet/Opus/Fable nuevos; Haiku usa la básica
        search_type = "web_search_20250305" if haiku else "web_search_20260209"
        base_kwargs["tools"] = [{"type": search_type, "name": "web_search", "max_uses": 6}]
    # Fable y Opus 5.x: si un clasificador de seguridad rechaza el pedido, el servidor reintenta
    # solo con otro modelo (server-side fallback) en vez de devolver un guión vacío.
    use_fallback = model.startswith(("claude-fable", "claude-opus-5"))

    full_text = ""
    search_count = 0
    bar_target = word_target * 6

    def stream_turn():
        nonlocal full_text, search_count
        last_update = 0
        if use_fallback:
            ctx = client.beta.messages.stream(messages=messages, betas=["server-side-fallback-2026-07-01"],
                                              extra_body={"fallbacks": "default"}, **base_kwargs)
        else:
            ctx = client.messages.stream(messages=messages, **base_kwargs)
        with ctx as stream:
            for event in stream:
                et = getattr(event, "type", None)
                if et == "content_block_start":
                    block = getattr(event, "content_block", None)
                    if block and getattr(block, "type", None) == "server_tool_use" \
                            and getattr(block, "name", None) == "web_search":
                        search_count += 1
                        _job_update(job["id"],
                                    progress_text=f"🔎 Buscando en la web ({search_count})...",
                                    search_count=search_count)
                elif et == "content_block_delta":
                    delta = getattr(event, "delta", None)
                    if delta and getattr(delta, "type", None) == "text_delta":
                        full_text += delta.text
                        if len(full_text) - last_update > 600:
                            words = round(len(full_text) / 6)
                            pct = min(int(len(full_text) / bar_target * 80), 80)
                            search_info = f" · 🔎{search_count}" if search_count else ""
                            _job_update(job["id"],
                                        progress_pct=pct, text_chars=len(full_text),
                                        progress_text=f"Escribiendo · {words} palabras{search_info}")
                            last_update = len(full_text)

    stream_turn()

    # Loop de continuación: Haiku suele cerrar el tema antes de llegar al largo pedido.
    # Si quedó corto, le pedimos que siga desarrollando hasta ~alcanzar el target.
    tries = 0
    while round(len(full_text) / 6) < word_target * 0.9 and tries < 4:
        tries += 1
        remaining = word_target - round(len(full_text) / 6)
        messages.append({"role": "assistant", "content": full_text})
        messages.append({"role": "user", "content": (
            f"Seguí desarrollando exactamente el mismo tema, sin repetir lo ya dicho y sin "
            f"concluir todavía. Faltan unas {remaining} palabras. Agregá más ejemplos, "
            f"anécdotas, datos concretos, contexto histórico y matices. Continuá el hilo "
            f"directamente, sin frases de transición tipo 'siguiendo con' ni saludos.")})
        before = len(full_text)
        full_text += "\n\n"
        stream_turn()
        if len(full_text) - before < 300:  # el modelo ya no aporta más: cortamos
            break

    return full_text, search_count

def _synth_chunk(args):
    chunk, voice, speed = args
    import time
    for attempt in range(3):
        r = requests.post(
            f"https://texttospeech.googleapis.com/v1beta1/text:synthesize?key={GOOGLE_TTS_KEY}",
            json={
                "input": {"text": chunk},
                "voice": {"languageCode": "es-US", "name": f"es-US-Chirp3-HD-{voice}"},
                "audioConfig": {"audioEncoding": "MP3", "speakingRate": speed},
            },
            timeout=60,
        )
        if r.status_code in (429, 500, 503) and attempt < 2:
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        return base64.b64decode(r.json()["audioContent"])

def _generate_audio(job, text):
    chunks = _split_text(_clean_markdown(text))
    _job_update(job["id"], status="recording", progress_pct=82,
                progress_text=f"Grabando {len(chunks)} partes en paralelo...")

    args = [(c, job["voice"], job["speed"]) for c in chunks]
    with ThreadPoolExecutor(max_workers=min(8, len(chunks))) as pool:
        parts = list(pool.map(_synth_chunk, args))

    audio = b"".join(parts)
    key = f"{S3_PREFIX}{job['id']}.mp3"
    _job_update(job["id"], progress_pct=96, progress_text="Guardando en nube...")
    _s3_put(key, audio)
    return key, len(audio)

def _gemini_title(source):
    """Genera un título corto y específico con Gemini a partir del prompt/texto.
    Devuelve None si no hay key o si la llamada falla (el caller usa un fallback).
    gemini-2.5-flash es un modelo 'thinking': con thinkingBudget=0 responde directo
    y no gasta el presupuesto de tokens pensando (evita respuestas vacías)."""
    if not GEMINI_API_KEY:
        return None
    context = (source or "").strip()[:800]
    if not context:
        return None
    try:
        resp = requests.post(
            "https://generativelanguage.googleapis.com/v1beta/models/"
            "gemini-2.5-flash-lite:generateContent",
            params={"key": GEMINI_API_KEY},
            json={
                "contents": [{"parts": [{"text":
                    'Generá un título en español de hasta 10 palabras para este podcast. '
                    'El título debe ser MUY ESPECÍFICO: incluí los nombres propios, palabras '
                    'clave y términos exactos del tema. Nunca uses títulos genéricos. Solo el '
                    'título, sin comillas ni puntuación final.\n\nContenido:\n' + context
                }]}],
                "generationConfig": {
                    "thinkingConfig": {"thinkingBudget": 0},
                    "maxOutputTokens": 40,
                    "temperature": 0.4,
                },
            },
            timeout=20,
        )
        if not resp.ok:
            print("gemini_title HTTP", resp.status_code, resp.text[:300])
            return None
        data = resp.json()
        cand = (data.get("candidates") or [None])[0] or {}
        parts = ((cand.get("content") or {}).get("parts")) or []
        title = "".join(p.get("text", "") for p in parts).strip().strip('"\'«».').strip()
        if title and len(title) < 100:
            return title
        print("gemini_title vacío. finishReason=", cand.get("finishReason"))
        return None
    except Exception as e:
        print("gemini_title error", repr(e))
        return None

# ─── Portadas ─────────────────────────────────────────────────────
# Primero se busca una imagen REAL del tema (foto de la banda o tapa del disco en Deezer, foto del
# artículo de Wikipedia de la persona/lugar/empresa). Sólo si no hay nada real se genera una
# ilustración con Gemini. Qué buscar lo decide Claude Haiku leyendo el título y el comienzo del tema.
COVER_MODEL = "gemini-2.5-flash-image"
IMG_HEADERS = {"User-Agent": "NarradorPodcasts/1.0 (pablofernandez1983@gmail.com)"}

COVER_PLAN_PROMPT = """Vas a elegir la imagen de portada de un episodio de podcast. Tiene que ser una imagen REAL
del tema principal, no una ilustración. Respondé SOLO un JSON (sin texto antes ni después): una lista de
1 a 3 opciones en orden de preferencia (si la primera no tiene imagen se prueba la siguiente), cada una así:
[{"source": "album" | "artist" | "wikipedia",
  "artist": "banda o artista (si source es album o artist)",
  "album": "título exacto del disco (si source es album)",
  "wikipedia_title": "título exacto del artículo de Wikipedia (si source es wikipedia)",
  "lang": "es" | "en"}]
Si no hay ninguna imagen real posible, respondé [].
Criterios:
- Si el episodio trata de un disco puntual (o de las letras de ciertos discos), "album" con el disco más representativo.
- Si trata de una banda o músico en general (historia, curiosidades), "artist".
- Si trata de una persona, lugar, empresa, obra o hecho histórico con artículo en Wikipedia, "wikipedia" con el
  título del artículo (en el idioma de "lang"; preferí "es" si existe).
- Agregá alternativas razonables: por ejemplo, para una banda, también su artículo de Wikipedia; para una
  persona poco conocida, el artículo del hecho o lugar principal; para un tema general, el artículo más cercano.
- Si el tema es abstracto (consejos, capacitación interna, ideas sueltas), respondé [].

Título: {title}

Comienzo del tema:
{context}"""

def _cover_plan(job):
    """Le pide a Claude Haiku qué imagen real buscar. Devuelve dict (o {"source": "none"})."""
    import json as _json
    from anthropic import Anthropic
    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    msg = client.messages.create(
        model="claude-haiku-4-5", max_tokens=300,
        messages=[{"role": "user", "content": COVER_PLAN_PROMPT
                   .replace("{title}", job.get("title") or "")
                   .replace("{context}", (job.get("prompt") or "")[:2500])}],
    )
    txt = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")
    m = re.search(r"\[.*\]", txt, re.S)
    try:
        plan = _json.loads(m.group(0)) if m else []
    except ValueError:
        plan = []
    return [c for c in plan if isinstance(c, dict)]

def _norm(t):
    import unicodedata
    t = unicodedata.normalize("NFKD", t or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", t).strip()

def _deezer_artist(name, topic=""):
    """El artista pedido; si el tema nombra a otro de los resultados con un nombre más completo
    (ej. pidió "La Mariposa" y el tema habla de "El Plan de la Mariposa"), gana ese."""
    r = requests.get("https://api.deezer.com/search/artist", params={"q": name}, timeout=20).json()
    found = [a for a in r.get("data", []) if a.get("picture_xl")]
    in_topic = [a for a in found if topic and f" {_norm(a['name'])} " in f" {topic} "]
    if in_topic:
        return max(in_topic, key=lambda a: len(a["name"]))
    return next((a for a in found if _norm(a["name"]) == _norm(name)), None)

def _mentioned(name, topic):
    """True si el nombre encontrado (artista, artículo) aparece en el tema del episodio: evita portadas de
    homónimos o de artículos que la búsqueda trae 'por parecido' (ej. otra banda con nombre similar)."""
    words = [w for w in _norm(re.sub(r"\(.*?\)", "", name)).split() if len(w) > 2]
    if not words:
        return False
    return sum(w in topic for w in words) / len(words) >= 0.6

def _find_real_image(plan, topic=""):
    """(url, descripción de la fuente) o (None, None). `topic` = título + tema normalizados."""
    src = plan.get("source")
    if src == "album" and plan.get("artist") and plan.get("album"):
        a = _deezer_artist(plan["artist"], topic)
        if a and _mentioned(a["name"], topic):
            albums = requests.get(f"https://api.deezer.com/artist/{a['id']}/albums",
                                  params={"limit": 100}, timeout=20).json().get("data", [])
            want = _norm(plan["album"])
            # Primero el disco exacto de estudio; si no, el que contenga el nombre
            for exact in (True, False):
                for al in albums:
                    t = _norm(al["title"])
                    if (t == want if exact else want in t) and al.get("cover_xl"):
                        return al["cover_xl"], f'Deezer: tapa de "{al["title"]}"'
        src = "artist"  # no apareció el disco: al menos la foto de la banda
    if src == "artist" and plan.get("artist"):
        a = _deezer_artist(plan["artist"], topic)
        if a and _mentioned(a["name"], topic):
            return a["picture_xl"], f'Deezer: foto de {a["name"]}'
    if src == "wikipedia" and plan.get("wikipedia_title"):
        for lang in [plan.get("lang") or "es", "en"]:
            api = f"https://{lang}.wikipedia.org/w/api.php"
            base = {"action": "query", "prop": "pageimages", "piprop": "thumbnail", "pithumbsize": 800, "format": "json"}
            # 1) el artículo exacto que eligió Claude; 2) si no existe, el más parecido, pero sólo si
            #    su título aparece en el tema (la búsqueda "por parecido" trae cualquier cosa si no)
            exact = requests.get(api, headers=IMG_HEADERS, timeout=20,
                                 params={**base, "titles": plan["wikipedia_title"], "redirects": 1}).json()
            search = requests.get(api, headers=IMG_HEADERS, timeout=20, params={
                **base, "generator": "search", "gsrsearch": plan["wikipedia_title"], "gsrlimit": 1}).json()
            for r, validate in ((exact, False), (search, True)):
                for pg in r.get("query", {}).get("pages", {}).values():
                    url = pg.get("thumbnail", {}).get("source")
                    if not url:
                        continue
                    if validate and not _mentioned(pg.get("title", ""), topic):
                        continue
                    return url, f'Wikipedia ({lang}): {pg.get("title")}'
    return None, None

def _gemini_illustration(job):
    context = (job.get("prompt") or "")[:600]
    prompt = (
        f'Ilustración cuadrada para la portada de un episodio de podcast titulado "{job.get("title") or ""}". '
        f"Tema del episodio: {context}\n\n"
        "Estilo: ilustración editorial moderna, colores cálidos, composición simple y reconocible en tamaño chico. "
        "Sin texto, sin letras, sin números, sin logos."
    )
    r = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/{COVER_MODEL}:generateContent",
        params={"key": GEMINI_API_KEY},
        json={"contents": [{"parts": [{"text": prompt}]}],
              "generationConfig": {"responseModalities": ["IMAGE"], "imageConfig": {"aspectRatio": "1:1"}}},
        timeout=120,
    )
    r.raise_for_status()
    parts = r.json()["candidates"][0]["content"]["parts"]
    return next(base64.b64decode(p["inlineData"]["data"]) for p in parts if "inlineData" in p)

def _square_jpeg(raw):
    """Recorte cuadrado centrado (un poco hacia arriba, donde suelen estar las caras) a 512x512 JPEG."""
    from io import BytesIO
    from PIL import Image
    img = Image.open(BytesIO(raw)).convert("RGB")
    w, h = img.size
    side = min(w, h)
    left = (w - side) // 2
    top = max(0, min(h - side, int((h - side) * 0.3)))
    img = img.crop((left, top, left + side, top + side)).resize((512, 512), Image.LANCZOS)
    out = BytesIO()
    img.save(out, "JPEG", quality=85)
    return out.getvalue()

def _make_cover(jid, allow_ai=True, force_ai=False):
    """Portada del episodio: imagen real si la hay; si no, ilustración con Gemini.
    Nunca rompe el job: si falla todo, el episodio usa la imagen por defecto."""
    job = _job_get(jid)
    if not job:
        return None
    raw, source = None, None
    try:
        if force_ai:
            raise LookupError("se pidió ilustración")
        url = None
        topic = _norm(f'{job.get("title") or ""} {(job.get("prompt") or "")[:6000]}')
        for cand in _cover_plan(job):
            url, source = _find_real_image(cand, topic)
            if url:
                break
        if url:
            r = requests.get(url, headers=IMG_HEADERS, timeout=30)
            r.raise_for_status()
            raw = r.content
    except LookupError:
        raw = None
    except Exception:
        traceback.print_exc()
        raw = None
    if raw is None:
        if not (allow_ai and GEMINI_API_KEY):
            return None
        raw, source = _gemini_illustration(job), "Ilustración generada con IA"
    # La key cambia en cada versión para que la app no muestre la portada vieja cacheada
    key = f"{S3_PREFIX}covers/{jid}-{int(datetime.now(timezone.utc).timestamp())}.jpg"
    _s3_put(key, _square_jpeg(raw), "image/jpeg")
    old = job.get("cover_key")
    _job_update(jid, cover_key=key, cover_source=source)
    if old and old != key:
        try:
            _s3_delete(old)
        except Exception:
            pass
    return key

def _make_cover_async(jid):
    def run():
        try:
            _make_cover(jid)
        except Exception:
            traceback.print_exc()
    threading.Thread(target=run, daemon=True).start()

def _process_job(jid):
    try:
        job = _job_get(jid)
        if not job:
            return

        # Título con Gemini apenas arranca (aparece rápido vía polling).
        # Si falla, se usa el fallback de primera línea más abajo.
        # Si el pedido ya trae título (ej. episodios de una serie compartidos desde la app), se respeta.
        gtitle = job.get("title") or _gemini_title(job["prompt"])
        if gtitle and not job.get("title"):
            _job_update(jid, title=gtitle)

        if job["model"] == "direct":
            text = job["prompt"]
            title = gtitle or text.split("\n")[0][:120].strip() or "Audio"
            _job_update(jid, status="recording", progress_pct=10,
                        progress_text="Grabando audio...", title=title)
        else:
            status_init = "writing"
            text_init = "🔎 Investigando en la web..." if job["web_search"] else "Pidiendo a Claude..."
            _job_update(jid, status=status_init, progress_pct=5, progress_text=text_init)
            text, searches = _generate_text(job)
            title = gtitle or text.split("\n")[0][:120].strip() or job["prompt"][:80]
            _job_update(jid, text_chars=len(text), title=title, search_count=searches)

        audio_key, audio_size = _generate_audio(job, text)
        _job_update(jid, status="done", progress_pct=100,
                    progress_text="Listo", audio_key=audio_key, audio_size=audio_size)
        _make_cover_async(jid)

    except Exception as e:
        traceback.print_exc()
        _job_update(jid, status="error", error=str(e)[:500])

# ─── Endpoints ────────────────────────────────────────────────────
@app.route("/")
def root():
    return jsonify({"ok": True, "service": "narrador-api"})

PRIVACY_HTML = """<!doctype html><html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Narrador — Política de privacidad</title>
<style>body{font-family:system-ui,sans-serif;max-width:680px;margin:40px auto;padding:0 20px;line-height:1.6;color:#222}</style>
</head><body>
<h1>Narrador — Política de privacidad</h1>
<p>Última actualización: 26 de septiembre de 2026.</p>
<p>Narrador es una app de uso personal para escuchar podcasts generados por su propio autor.</p>
<h2>Qué datos usa</h2>
<ul>
<li>La lista de episodios, sus carpetas y el progreso de escucha (posición y si ya se escuchó) se guardan en el servidor propio de la app para poder retomarlos desde cualquier dispositivo.</li>
<li>Los archivos de audio se descargan al teléfono para escucharlos sin conexión.</li>
</ul>
<h2>Qué datos NO usa</h2>
<p>La app no pide cuentas ni datos personales, no usa ubicación, contactos, micrófono ni cámara, no muestra publicidad y no comparte información con terceros.</p>
<h2>Contacto</h2>
<p>pablofernandez1983@gmail.com</p>
</body></html>"""

@app.route("/privacy")
def privacy():
    return Response(PRIVACY_HTML, mimetype="text/html")

@app.route("/health")
def health():
    return jsonify({"ok": True})

@app.route("/jobs", methods=["POST"])
def jobs_create():
    if (err := _check_auth()):
        return err
    body   = request.get_json(force=True, silent=True) or {}
    prompt = (body.get("prompt") or "").strip()
    text   = (body.get("text")   or "").strip()

    if not prompt and not text:
        return jsonify({"error": "prompt o text requerido"}), 400

    if text:
        data = {
            "prompt":     text,
            "model":      "direct",
            "voice":      body.get("voice") or "Achird",
            "speed":      float(body.get("speed") or 1.0),
            "web_search": False,
        }
    else:
        data = {
            "prompt":     prompt,
            "model":      body.get("model") or "claude-haiku-4-5",
            "voice":      body.get("voice") or "Achird",
            "speed":      float(body.get("speed") or 1.0),
            "web_search": bool(body.get("web_search")),
        }
    data["folder"] = (body.get("folder") or "").strip() or None
    data["title"] = (body.get("title") or "").strip()[:200] or None
    if data["folder"]:
        _folder_ensure(data["folder"])

    jid = _job_create(data)
    threading.Thread(target=_process_job, args=(jid,), daemon=True).start()
    return jsonify(_job_get(jid)), 201

@app.route("/jobs", methods=["GET"])
def jobs_list():
    if (err := _check_auth()):
        return err
    limit = max(1, min(int(request.args.get("limit") or 50), 1000))
    return jsonify(_job_list(limit))

@app.route("/jobs/<jid>", methods=["GET"])
def jobs_get(jid):
    if (err := _check_auth()):
        return err
    job = _job_get(jid)
    if not job:
        return jsonify({"error": "not found"}), 404
    return jsonify(job)

@app.route("/jobs/<jid>/audio", methods=["GET"])
def jobs_audio(jid):
    if (err := _check_auth()):
        return err
    job = _job_get(jid)
    if not job or not job.get("audio_key"):
        return jsonify({"error": "audio no disponible"}), 404

    safe = re.sub(r'\s+', '_', re.sub(r'[^\w\s-]', '', job.get("title") or "narrador")).lower()[:60] or "narrador"
    headers = {"Content-Disposition": f'inline; filename="{safe}.mp3"', "Accept-Ranges": "bytes"}
    rng = request.headers.get("Range", "")
    if rng.startswith("bytes="):
        # Pedido parcial (streaming con seek desde la app / el <audio> del navegador)
        audio, content_range = _s3_get_range(job["audio_key"], rng)
        if content_range:
            headers["Content-Range"] = content_range
        return Response(audio, status=206, mimetype="audio/mpeg", headers=headers)
    audio = _s3_get(job["audio_key"])
    return Response(audio, mimetype="audio/mpeg", headers=headers)

@app.route("/covers/<jid>.jpg", methods=["GET"])
def cover_get(jid):
    """Portada del episodio. Sin clave a propósito: la pide Android Auto / el cargador de imágenes
    y el id es un UUID imposible de adivinar; no expone nada más que la ilustración."""
    job = _job_get(jid)
    if not job or not job.get("cover_key") or job.get("deleted_at"):
        return jsonify({"error": "sin portada"}), 404
    data = _s3().get_object(Bucket=S3_BUCKET, Key=job["cover_key"])["Body"].read()
    return Response(data, mimetype="image/jpeg", headers={"Cache-Control": "public, max-age=604800"})

@app.route("/jobs/<jid>/cover", methods=["POST"])
def jobs_cover(jid):
    """(Re)genera la portada de un episodio existente."""
    if (err := _check_auth()):
        return err
    if not _job_get(jid):
        return jsonify({"error": "not found"}), 404
    body = request.get_json(force=True, silent=True) or {}
    try:
        _make_cover(jid, force_ai=bool(body.get("ai")))
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"no se pudo generar: {str(e)[:200]}"}), 502
    return jsonify(_job_get(jid))

# ─── Biblioteca: progreso y carpetas (app Android) ────────────────
JOB_PATCHABLE = {"folder", "position_ms", "duration_ms", "listened", "title"}

@app.route("/jobs/<jid>", methods=["PATCH"])
def jobs_patch(jid):
    if (err := _check_auth()):
        return err
    body = request.get_json(force=True, silent=True) or {}
    fields = {k: v for k, v in body.items() if k in JOB_PATCHABLE}
    if "folder" in fields:
        fields["folder"] = (fields["folder"] or "").strip() or None
        if fields["folder"]:
            _folder_ensure(fields["folder"])
    if "position_ms" in fields:
        fields["played_at"] = datetime.now(timezone.utc)
    if not fields:
        return jsonify({"error": "nada para actualizar"}), 400
    if not _job_get(jid):
        return jsonify({"error": "not found"}), 404
    _job_update(jid, **fields)
    return jsonify(_job_get(jid))

def _folder_ensure(name):
    c, _ = _conn()
    try:
        with c.cursor() as cur:
            cur.execute("""
                INSERT INTO narrador_folders (name, position)
                VALUES (%s, (SELECT COALESCE(MAX(position), 0) + 1 FROM narrador_folders))
                ON CONFLICT (name) DO NOTHING
            """, (name,))
        c.commit()
    finally:
        c.close()

def _folder_list():
    c, RDC = _conn()
    try:
        with c.cursor(cursor_factory=RDC) as cur:
            cur.execute("SELECT name, position, sort FROM narrador_folders ORDER BY position, name")
            return [dict(r) for r in cur.fetchall()]
    finally:
        c.close()

@app.route("/folders", methods=["GET"])
def folders_list():
    if (err := _check_auth()):
        return err
    return jsonify(_folder_list())

@app.route("/folders", methods=["POST"])
def folders_create():
    if (err := _check_auth()):
        return err
    name = ((request.get_json(force=True, silent=True) or {}).get("name") or "").strip()
    if not name:
        return jsonify({"error": "name requerido"}), 400
    _folder_ensure(name)
    return jsonify(_folder_list()), 201

@app.route("/folders/<path:name>", methods=["PATCH"])
def folders_patch(name):
    """Renombrar (arrastra los episodios), cambiar el orden de la carpeta o su posición."""
    if (err := _check_auth()):
        return err
    body = request.get_json(force=True, silent=True) or {}
    new_name = (body.get("name") or "").strip()
    c, _ = _conn()
    try:
        with c.cursor() as cur:
            if body.get("sort") in ("asc", "desc"):
                cur.execute("UPDATE narrador_folders SET sort = %s WHERE name = %s", (body["sort"], name))
            if isinstance(body.get("position"), int):
                cur.execute("UPDATE narrador_folders SET position = %s WHERE name = %s", (body["position"], name))
            if new_name and new_name != name:
                cur.execute("UPDATE narrador_folders SET name = %s WHERE name = %s", (new_name, name))
                cur.execute("UPDATE narrador_jobs SET folder = %s WHERE folder = %s", (new_name, name))
        c.commit()
    finally:
        c.close()
    return jsonify(_folder_list())

@app.route("/folders/<path:name>", methods=["DELETE"])
def folders_delete(name):
    """Borra la carpeta; sus episodios vuelven a 'Nuevos' (folder NULL), no se borran."""
    if (err := _check_auth()):
        return err
    c, _ = _conn()
    try:
        with c.cursor() as cur:
            cur.execute("UPDATE narrador_jobs SET folder = NULL WHERE folder = %s", (name,))
            cur.execute("DELETE FROM narrador_folders WHERE name = %s", (name,))
        c.commit()
    finally:
        c.close()
    return jsonify(_folder_list())

@app.route("/synth", methods=["POST"])
def synth():
    if (err := _check_auth()): return err
    body = request.get_json(force=True, silent=True) or {}
    text  = (body.get("text") or "").strip()
    voice = body.get("voice") or "Achird"
    speed = float(body.get("speed") or 1.0)
    if not text: return jsonify({"error": "text requerido"}), 400

    chunks = _split_text(_clean_markdown(text))
    args = [(c, voice, speed) for c in chunks]
    with ThreadPoolExecutor(max_workers=min(8, len(chunks))) as pool:
        parts = list(pool.map(_synth_chunk, args))

    audio = b"".join(parts)
    return Response(audio, mimetype="audio/mpeg")

@app.route("/jobs/<jid>", methods=["DELETE"])
def jobs_delete(jid):
    if (err := _check_auth()):
        return err
    job = _job_get(jid)
    if not job:
        return jsonify({"error": "not found"}), 404
    # Borrado lógico: el 27-sep-2026 el robot de pruebas de Google Play borró 16 episodios
    # desde la app Android y no había forma de recuperarlos. Ahora sólo se ocultan
    # (audio y fila quedan); se restauran con UPDATE narrador_jobs SET deleted_at = NULL.
    _job_update(jid, deleted_at=datetime.now(timezone.utc))
    return jsonify({"ok": True})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
