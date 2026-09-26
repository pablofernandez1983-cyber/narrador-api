# Narrador API — Backend

## Instrucciones para agentes

- Usá este archivo como contexto principal del proyecto cuando trabajes en esta carpeta.
- CLAUDE.md es la fuente original de contexto y no debe modificarse salvo pedido explícito del usuario.
- Antes de hacer cambios, revisá los archivos clave y respetá el stack, flujo y gotchas documentados abajo.
- No expongas secretos ni muevas credenciales a código cliente. Las variables de entorno mencionadas son referencias de configuración.
- Mantené los cambios acotados al objetivo pedido y verificá con los comandos o flujos locales disponibles en este proyecto.

## Contexto del proyecto

# Narrador API — Backend

Backend Flask que genera podcasts en español rioplatense. Toma un prompt, genera texto con Claude, lo sintetiza con Google TTS Chirp 3 HD, y guarda el MP3 en S3 (Tigris).

## Stack

- Flask + CORS, Gunicorn (1 worker, 4 threads)
- PostgreSQL via Supabase (`DATABASE_URL`)
- Anthropic SDK (Claude Haiku por default, soporta web_search)
- Google TTS API — voces Chirp 3 HD
- Tigris S3 — almacena MP3s bajo `narrador/<uuid>.mp3`
- Deploy: Railway

## Endpoints

| Método | Ruta | Descripción |
|--------|------|-------------|
| `POST` | `/jobs` | Crear job (auth requerida) |
| `GET` | `/jobs` | Listar cola |
| `GET` | `/jobs/<id>` | Estado del job |
| `GET` | `/jobs/<id>/audio` | Descargar MP3 |
| `POST` | `/synth` | Sintetizar texto directo (sin IA) |
| `DELETE` | `/jobs/<id>` | Borrar job y audio de S3 |

## Variables de entorno

```
DATABASE_URL
ANTHROPIC_API_KEY
GOOGLE_TTS_API_KEY
NARRADOR_API_KEY        ← contraseña que valida el frontend
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_ENDPOINT_URL        ← Tigris endpoint
AWS_S3_BUCKET_NAME
AWS_DEFAULT_REGION
```

## Gotchas

**Job processing en thread daemon** — sin queue real. Si Railway reinicia mid-job, el job queda en estado `recording` para siempre.

**Chunking de Google TTS**: textos largos se parten en chunks de 3.8KB UTF-8 — cambiar ese límite puede romper síntesis.

**word_target con regex**: la duración estimada del podcast se infiere del prompt con regex. Si el prompt no menciona duración, usa un default.

**Markdown se limpia antes de TTS**: Claude genera con formato, el backend stripea antes de sintetizar. Si Claude cambia su formato de salida puede afectar el audio.

## Frontend asociado

`C:\claude\narrador\` — ver su CLAUDE.md.
