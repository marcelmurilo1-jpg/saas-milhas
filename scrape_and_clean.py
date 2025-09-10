#!/usr/bin/env python3
# scrape_and_clean.py
# Coleta posts + calcula validade + limpa expirados (com backup).

import os
import re
import json
import time
import psycopg2
import unicodedata
import feedparser
import requests
import dateparser
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv

# -------- CONFIG --------
SITE = "https://passageirodeprimeira.com"
RSS_URL = SITE + "/feed/"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
TZ = ZoneInfo("America/Sao_Paulo")
RATE_SECONDS = 1.5
REQUEST_TIMEOUT = 20
BACKUP_RETENTION_DAYS = 30
# ------------------------

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL não encontrado no .env")

def db_connect():
    return psycopg2.connect(DB_URL)

# --------- VALID UNTIL PARSER ---------
def parse_valid_until(text, published_dt):
    """
    Extrai data/hora de expiração do texto do post, usando published_dt como âncora.
    Sempre retorna None em caso de erro, nunca levanta exceção.
    """
    try:
        if not text or not published_dt:
            return None

        txt = unicodedata.normalize("NFKC", text.lower())
        txt = re.sub(r"\s+", " ", txt).strip()

        # usar dateparser direto
        found = dateparser.search.search_dates(
            txt,
            settings={
                "RELATIVE_BASE": published_dt,
                "PREFER_DATES_FROM": "future",
                "DATE_ORDER": "DMY",
                "RETURN_AS_TIMEZONE_AWARE": True
            }
        )
        if found:
            for _, cand in found:
                if cand:
                    return cand.astimezone(TZ)

    except Exception:
        return None

    return None

# --------- SCRAPER ---------
def safe_get_text(el):
    return el.get_text(strip=True) if el else None

def extrair_conteudo(url, feed_title=None, published_dt=None):
    headers = {"User-Agent": USER_AGENT, "Referer": SITE}
    resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    titulo_tag = soup.find("h1") or soup.find("h2")
    title = safe_get_text(titulo_tag) or feed_title or "Sem título"

    author_tag = soup.find(attrs={"rel": "author"}) or soup.find(class_="author")
    author = safe_get_text(author_tag)

    content_soup = soup.select_one("div.td-post-content") or soup.find("article") or soup.body
    for bad in content_soup.find_all(['script','style','iframe','noscript']):
        bad.decompose()

    content_text = content_soup.get_text(" ", strip=True)
    content_html = str(content_soup)

    images = []
    for img in content_soup.find_all("img"):
        src = img.get("src") or img.get("data-src")
        if src:
            images.append({"src": urljoin(url, src), "alt": img.get("alt", "")})

    links = []
    for a in content_soup.find_all("a", href=True):
        links.append({"href": urljoin(url, a["href"]), "text": a.get_text(" ", strip=True)})

    valid_until = parse_valid_until(content_text, published_dt)

    return {
        "url": url,
        "title": title,
        "date_published": published_dt.date().isoformat() if published_dt else None,
        "author": author,
        "content_text": content_text,
        "content_html": content_html,
        "images": images,
        "links": links,
        "valid_until": valid_until.isoformat() if valid_until else None
    }

def posts_de_hoje():
    feed = feedparser.parse(RSS_URL)
    hoje = datetime.now(TZ).date()
    items = []
    for entry in feed.entries:
        pub_dt = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            pub_dt = datetime(*entry.published_parsed[:6], tzinfo=TZ)
        elif hasattr(entry, "published"):
            try:
                pub_dt = dateparser.parse(entry.published)
                if pub_dt and not pub_dt.tzinfo:
                    pub_dt = pub_dt.replace(tzinfo=TZ)
            except Exception:
                pass
        if pub_dt and pub_dt.date() == hoje:
            items.append({"link": entry.link, "feed_title": entry.get("title"), "published": pub_dt})
    return items

# --------- DB FUNCTIONS ---------
def upsert_post(conn, data):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO promocoes (url, title, date_published, author, content_text,
                               content_html, images_json, links_json, valid_until, scraped_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (url) DO UPDATE SET
            title=EXCLUDED.title,
            author=EXCLUDED.author,
            content_text=EXCLUDED.content_text,
            content_html=EXCLUDED.content_html,
            images_json=EXCLUDED.images_json,
            links_json=EXCLUDED.links_json,
            valid_until=EXCLUDED.valid_until,
            scraped_at=NOW()
    """, (
        data["url"], data["title"], data["date_published"], data["author"],
        data["content_text"], data["content_html"],
        json.dumps(data["images"], ensure_ascii=False),
        json.dumps(data["links"], ensure_ascii=False),
        data["valid_until"]
    ))
    conn.commit()
    cur.close()

def move_and_delete_expired(conn):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO promocoes_backup (url, title, date_published, author, content_text, content_html,
                                     images_json, links_json, valid_until, scraped_at, deleted_at)
        SELECT url, title, date_published, author, content_text, content_html,
               images_json, links_json, valid_until, scraped_at, NOW()
        FROM promocoes
        WHERE valid_until IS NOT NULL
          AND valid_until < NOW()
        ON CONFLICT (url) DO UPDATE SET
            title=EXCLUDED.title,
            author=EXCLUDED.author,
            content_text=EXCLUDED.content_text,
            content_html=EXCLUDED.content_html,
            images_json=EXCLUDED.images_json,
            links_json=EXCLUDED.links_json,
            valid_until=EXCLUDED.valid_until,
            scraped_at=EXCLUDED.scraped_at,
            deleted_at=NOW()
    """)
    moved = cur.rowcount

    cur.execute("""
        DELETE FROM promocoes
        WHERE valid_until IS NOT NULL
          AND valid_until < NOW()
    """)
    deleted = cur.rowcount

    cur.execute("""
        DELETE FROM promocoes_backup
        WHERE deleted_at IS NOT NULL
          AND deleted_at < NOW() - INTERVAL '%s days'
    """, (BACKUP_RETENTION_DAYS,))
    cleaned = cur.rowcount

    conn.commit()
    cur.close()
    return moved, deleted, cleaned

# --------- MAIN ---------
def main():
    print("🚀 Rodando coleta + limpeza integrada...")
    items = posts_de_hoje()
    print(f"📌 {len(items)} posts de hoje encontrados no RSS.")

    conn = db_connect()
    for i, it in enumerate(items, start=1):
        print(f"[{i}/{len(items)}] Processando: {it['link']}")
        try:
            data = extrair_conteudo(it["link"], feed_title=it.get("feed_title"), published_dt=it["published"])
            upsert_post(conn, data)
            print(f"   ✅ Salvo: {data['title']}  (valid_until={data['valid_until']})")
        except Exception as e:
            print(f"   ❌ Erro ao processar {it['link']}: {e}")
        time.sleep(RATE_SECONDS)

    print("\n🧹 Rodando backup+remoção de expirados...")
    try:
        moved, deleted, cleaned = move_and_delete_expired(conn)
        print(f"➡️  Movidos {moved} posts para backup, excluídos {deleted} da tabela principal.")
        print(f"🗑️  Removidos {cleaned} backups antigos (>{BACKUP_RETENTION_DAYS} dias).")
    except Exception as e:
        print("   ❌ Erro durante backup/limpeza:", e)

    conn.close()
    print("\n🏁 Finalizado.")

if __name__ == "__main__":
    main()
