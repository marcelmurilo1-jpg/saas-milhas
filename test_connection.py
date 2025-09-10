import psycopg2

DB_URL = "postgresql://postgres.mbwlpwmhzihgidwcyjax:%40Marcelmurilo1090@aws-1-sa-east-1.pooler.supabase.com:5432/postgres"

try:
    conn = psycopg2.connect(DB_URL)
    print("✅ Conexão bem-sucedida!")
    conn.close()
except Exception as e:
    print("❌ Erro na conexão:", e)
