#!/usr/bin/env python3
# delete_expired_dryrun.py
"""
Lista os registros expirados que seriam movidos ao backup e removidos.
"""

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise SystemExit("DATABASE_URL não encontrada no .env")

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

cur.execute("""
    SELECT id, url, title, valid_until
    FROM promocoes
    WHERE valid_until IS NOT NULL
      AND valid_until < NOW()
    ORDER BY valid_until ASC
    LIMIT 500;
""")
rows = cur.fetchall()
print(f"[dry-run] Encontrados {len(rows)} registros expirados (mostrando até 500):\n")
for r in rows:
    print(f"ID: {r[0]}  valid_until: {r[3]}  url: {r[1]}\n  título: {r[2]}\n")

cur.close()
conn.close()
