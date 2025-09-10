# test_conn.py
from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise SystemExit("DATABASE_URL não encontrada no .env")

print("Tentando conectar ao host:", DB_URL.split('@')[-1].split(':')[0])
conn = psycopg2.connect(DB_URL)
print("Conectado com sucesso!")
conn.close()
