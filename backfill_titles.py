#!/usr/bin/env python3
"""
Genera títulos para todos los jobs de la DB que no tienen title.
Uso: DATABASE_URL=... ANTHROPIC_API_KEY=... python backfill_titles.py
"""

import os
import sys
import time
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL      = os.environ.get("DATABASE_URL", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")


def generate_title(text):
    from anthropic import Anthropic
    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    msg = client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=60,
        messages=[{
            "role": "user",
            "content": (
                "Generá un título de hasta 10 palabras en español que describa "
                "claramente el tema del siguiente texto. El título debe ser informativo y "
                "directo, no poético ni creativo. Ejemplos del estilo esperado: "
                "\"Historia del tango rioplatense\", \"Cambio de figuritas Panini 2024\", "
                "\"Economía argentina en tiempos de inflación\". "
                "Respondé SOLO con el título, sin comillas, sin puntuación final.\n\n"
                f"Texto: {text[:500]}"
            ),
        }],
    )
    return msg.content[0].text.strip()[:200]


def main():
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL no configurado", file=sys.stderr)
        sys.exit(1)
    if not ANTHROPIC_API_KEY:
        print("ERROR: ANTHROPIC_API_KEY no configurado", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, prompt
            FROM narrador_jobs
            WHERE title IS NULL OR trim(title) = ''
            ORDER BY created_at ASC
        """)
        jobs = cur.fetchall()

    total = len(jobs)
    print(f"Jobs sin título: {total}")
    if not total:
        print("Nada que actualizar.")
        conn.close()
        return

    updated = errors = 0

    for job in jobs:
        jid    = str(job["id"])
        prompt = (job["prompt"] or "").strip()
        try:
            title = generate_title(prompt)
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE narrador_jobs SET title = %s, updated_at = now() WHERE id = %s",
                    (title, jid),
                )
            conn.commit()
            updated += 1
            print(f"[{updated}/{total}] {jid[:8]}...  →  {title!r}")
            time.sleep(0.25)  # evitar rate limits de Claude
        except Exception as e:
            errors += 1
            conn.rollback()
            print(f"ERROR {jid[:8]}: {e}", file=sys.stderr)

    conn.close()
    print(f"\nListo: {updated} actualizados, {errors} errores.")


if __name__ == "__main__":
    main()
