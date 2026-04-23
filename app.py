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
NARRADOR_API_KEY  = os.environ.get("NARRADOR_API_KEY", "")  # auth del backend

S3_BUCKET   = os.environ.get("AWS_S3_BUCKET_NAME", "")
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "")
S3_REGION   = os.environ.get("AWS_DEFAULT_REGION", "auto")
S3_KEY_ID   = os.environ.get("AWS_ACCESS_KEY_ID", "")
S3_SECRET   = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
S3_PREFIX   = "narrador/"

app = Flask(__name__)
CORS(app)

# ─── Auth ─────────────────────────────────────────────────────────
def _check_auth():
    if not NARRADOR_API_KEY:
        return None  # sin NARRADOR_API_KEY configurado, libre (dev local)
    if request.headers.get("X-API-Key", "") != NARRADOR_API_KEY:
        return jsonify({"error": "unauthorized"}), 401
    return None

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
    d["speed"] = float(d.get("speed") or 1.0)
    return d

def _job_create(data):
    jid = str(uuid.uuid4())
    c, RDC = _conn()
    try:
        with c.cursor() as cur:
            cur.execute("""
                INSERT INTO narrador_jobs
                  (id, prompt, model, voice, speed, web_search, status, progress_pct, search_count)
                VALUES (%s, %s, %s, %s, %s, %s, 'pending', 0, 0)
            """, (jid, data["prompt"], data["model"], data["voice"],
                  data["speed"], data["web_search"]))
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
            cur.execute("SELECT * FROM narrador_jobs ORDER BY created_at DESC LIMIT %s", (limit,))
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

def _s3_put(key, body):
    _s3().put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="audio/mpeg")

def _s3_get(key):
    return _s3().get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()

def _s3_delete(key):
    _s3().delete_object(Bucket=S3_BUCKET, Key=key)

# ─── Text utils ───────────────────────────────────────────────────
DURATION_RE = re.compile(
    r'(\d+(?:[.,]\d+)?)\s*(h|hs|hora|horas|min|mins|minuto|minutos)\b', re.I
)

def _word_target(prompt):
    m = DURATION_RE.search(prompt)
    if not m:
        return None
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
    "Respondé SIEMPRE con texto limpio listo para leer en voz alta: sin títulos, sin encabezados "
    "tipo 'Introducción', sin markdown, sin listas con viñetas, sin meta-comentarios tipo 'aquí tienes' "
    "o 'espero que te guste', sin citas tipo '[1]' ni URLs pegadas en el medio del texto. "
    "Solo párrafos naturales separados por líneas en blanco, con tono conversacional y fluido. "
    "Cuando el usuario pida una duración específica en minutos u horas, calculá el largo a ~155 "
    "palabras por minuto narradas y cumplilo — es mejor pasarse un poco que quedar corto."
)

# ─── Job processor ────────────────────────────────────────────────
def _generate_text(job):
    from anthropic import Anthropic
    client = Anthropic(api_key=ANTHROPIC_API_KEY)

    word_target = _word_target(job["prompt"])
    user_content = job["prompt"]
    if word_target:
        user_content += (
            f"\n\n[Instrucción de largo — CRÍTICA: apuntá a aproximadamente {word_target} palabras "
            f"para que dure lo pedido a ~155 palabras por minuto narradas. "
            f"Mejor pasarte un poco que quedar corto. No pares antes.]"
        )

    kwargs = dict(
        model=job["model"],
        max_tokens=32000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
    )
    if job["web_search"]:
        kwargs["tools"] = [{"type": "web_search_20250305", "name": "web_search", "max_uses": 6}]

    full_text = ""
    search_count = 0
    last_update = 0
    bar_target = (word_target or 4500) * 6  # chars estimados

    with client.messages.stream(**kwargs) as stream:
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

    return full_text, search_count

def _synth_chunk(args):
    chunk, voice, speed = args
    r = requests.post(
        f"https://texttospeech.googleapis.com/v1beta1/text:synthesize?key={GOOGLE_TTS_KEY}",
        json={
            "input": {"text": chunk},
            "voice": {"languageCode": "es-US", "name": f"es-US-Chirp3-HD-{voice}"},
            "audioConfig": {"audioEncoding": "MP3", "speakingRate": speed},
        },
        timeout=60,
    )
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

def _process_job(jid):
    try:
        job = _job_get(jid)
        if not job:
            return

        status_init = "writing"
        text_init = "🔎 Investigando en la web..." if job["web_search"] else "Pidiendo a Claude..."
        _job_update(jid, status=status_init, progress_pct=5, progress_text=text_init)

        text, searches = _generate_text(job)

        title = text.split("\n")[0][:120].strip() or job["prompt"][:80]
        _job_update(jid, text_chars=len(text), title=title, search_count=searches)

        audio_key, audio_size = _generate_audio(job, text)

        _job_update(jid, status="done", progress_pct=100,
                    progress_text="Listo", audio_key=audio_key, audio_size=audio_size)

    except Exception as e:
        traceback.print_exc()
        _job_update(jid, status="error", error=str(e)[:500])

# ─── Endpoints ────────────────────────────────────────────────────
@app.route("/")
def root():
    return jsonify({"ok": True, "service": "narrador-api"})

@app.route("/health")
def health():
    return jsonify({"ok": True})

@app.route("/jobs", methods=["POST"])
def jobs_create():
    if (err := _check_auth()):
        return err
    body = request.get_json() or {}
    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({"error": "prompt requerido"}), 400

    data = {
        "prompt": prompt,
        "model":      body.get("model") or "claude-haiku-4-5",
        "voice":      body.get("voice") or "Achird",
        "speed":      float(body.get("speed") or 1.0),
        "web_search": bool(body.get("web_search")),
    }
    jid = _job_create(data)
    threading.Thread(target=_process_job, args=(jid,), daemon=True).start()
    return jsonify(_job_get(jid)), 201

@app.route("/jobs", methods=["GET"])
def jobs_list():
    if (err := _check_auth()):
        return err
    return jsonify(_job_list())

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

    audio = _s3_get(job["audio_key"])
    safe = re.sub(r'\s+', '_', re.sub(r'[^\w\s-]', '', job.get("title") or "narrador")).lower()[:60] or "narrador"
    return Response(audio, mimetype="audio/mpeg",
                    headers={"Content-Disposition": f'inline; filename="{safe}.mp3"'})

@app.route("/jobs/<jid>", methods=["DELETE"])
def jobs_delete(jid):
    if (err := _check_auth()):
        return err
    job = _job_get(jid)
    if not job:
        return jsonify({"error": "not found"}), 404
    if job.get("audio_key"):
        try:
            _s3_delete(job["audio_key"])
        except Exception:
            pass
    _job_delete(jid)
    return jsonify({"ok": True})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
