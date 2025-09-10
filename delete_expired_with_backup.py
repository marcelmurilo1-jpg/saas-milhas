#!/usr/bin/env python3
# delete_expired_with_backup.py
# Remove posts expirados (valid_until < agora) e faz backup antes.

import os
import psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# -------- CONFIG --------
TZ = ZoneInfo("America/Sao_Paulo")
BACKUP_RETENTION_DAYS = 30  # tempo para manter backup
# ------------------------

# -------- DB CONFIG --------
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL não encontrado no .env")

def db_connect():
    return psycopg2.connect(DB_URL)

def init_backup_table(conn):
    """Cria tabela de backup se não existir"""
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS promocoes_backup (
        id SERIAL PRIMARY KEY,
        url TEXT,
        title TEXT,
        date_published DATE,
        author TEXT,
        content_text TEXT,
        content_html TEXT,
        images_json JSONB,
        links_json JSONB,
        scraped_at TIMESTAMPTZ,
        valid_until TIMESTAMPTZ,
        deleted_at TIMESTAMPTZ DEFAULT NOW()
    )
    """)
    conn.commit()

def move_expired(conn):
    """Move registros expirados para o backup"""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO promocoes_backup
            (url, title, date_published, author, content_text, content_html,
             images_json, links_json, scraped_at, valid_until, deleted_at)
        SELECT url, title, date_published, author, content_text, content_html,
               images_json, links_json, scraped_at, valid_until, NOW()
        FROM promocoes
        WHERE valid_until IS NOT NULL
          AND valid_until < NOW()
    """)
    moved = cur.rowcount

    cur.execute("""
        DELETE FROM promocoes
        WHERE valid_until IS NOT NULL
          AND valid_until < NOW()
    """)
    deleted = cur.rowcount
    conn.commit()
    return moved, deleted

def cleanup_old_backups(conn):
    """Remove backups antigos"""
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM promocoes_backup
        WHERE deleted_at < NOW() - INTERVAL %s
    """, (f"{BACKUP_RETENTION_DAYS} days",))
    removed = cur.rowcount
    conn.commit()
    return removed

def main():
    print("🧹 Iniciando limpeza de posts expirados...")
    conn = db_connect()
    init_backup_table(conn)

    moved, deleted = move_expired(conn)
    print(f"➡️  Movidos {moved} posts para backup e excluídos {deleted} da tabela principal.")

    removed = cleanup_old_backups(conn)
    print(f"🗑️  Removidos {removed} backups antigos (>{BACKUP_RETENTION_DAYS} dias).")

    conn.close()
    print("✅ Limpeza concluída.")

if __name__ == "__main__":
    main()
