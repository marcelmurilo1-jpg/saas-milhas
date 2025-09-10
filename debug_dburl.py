# debug_dburl.py
from dotenv import load_dotenv
from urllib.parse import urlparse
import os

load_dotenv()
db = os.getenv("DATABASE_URL")
print("Raw DATABASE_URL set?:", "YES" if db else "NO")
if not db:
    print("-> DATABASE_URL não encontrada no ambiente. Verifique o arquivo .env na mesma pasta.")
    raise SystemExit(1)

u = urlparse(db)
print(" scheme:", u.scheme)
print(" username:", u.username)
print(" hostname:", u.hostname)
print(" port:", u.port)
print(" path:", u.path)
