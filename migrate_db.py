# migrate_db.py
import sqlite3
import sys

DB = "promocoes.db"

expected = [
    ("author", "TEXT"),
    ("content_html", "TEXT"),
    ("images_json", "TEXT"),
    ("links_json", "TEXT"),
    ("scraped_at", "TEXT")
]

conn = sqlite3.connect(DB)
cur = conn.cursor()

# verificar se a tabela existe
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='promocoes';")
if not cur.fetchone():
    print("Tabela 'promocoes' não encontrada — nada a migrar. O script principal criará a tabela automaticamente.")
    conn.close()
    sys.exit(0)

# buscar colunas atuais
cur.execute("PRAGMA table_info(promocoes);")
existing = [row[1] for row in cur.fetchall()]

added = []
for col, coltype in expected:
    if col not in existing:
        print(f"Adicionando coluna: {col} {coltype}")
        cur.execute(f"ALTER TABLE promocoes ADD COLUMN {col} {coltype}")
        added.append(col)

conn.commit()
conn.close()

if added:
    print("Migração concluída. Colunas adicionadas:", added)
else:
    print("Nenhuma coluna adicional necessária. Banco já está atualizado.")
