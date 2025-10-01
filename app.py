import os
import time
import requests
import psycopg2
from datetime import datetime
from flask import Flask
import threading

# ====== CONFIG ======
API_URL = "https://www.vn58q.bet/api/minigame/games/PK3_60S/history100"
INTERVAL = 60  # fetch mỗi 60s
API_TOKEN = os.getenv("API_TOKEN")

def get_conn():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("❌ DATABASE_URL chưa được set")
    if "sslmode" not in dsn:
        if "?" in dsn:
            dsn += "&sslmode=require"
        else:
            dsn += "?sslmode=require"
    return psycopg2.connect(dsn)

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        issueId TEXT PRIMARY KEY,
        dice1 INT,
        dice2 INT,
        dice3 INT,
        point INT,
        result TEXT
    )
    """)
    conn.commit()
    cur.close()
    conn.close()

def save_to_db(sessions):
    conn = get_conn()
    cur = conn.cursor()
    for s in sessions:
        try:
            dices = [int(d) for d in s["result"]]
            total = sum(dices)
            result = "TAI" if total >= 11 else "XIU"

            cur.execute("""
                INSERT INTO sessions (issueId, dice1, dice2, dice3, point, result)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (issueId) DO NOTHING
            """, (s["issueId"], dices[0], dices[1], dices[2], total, result))
        except Exception as e:
            print("❌ Insert lỗi:", e)
    conn.commit()
    cur.close()
    conn.close()

def fetch_and_save():
    try:
        headers = {
            "accept": "application/json, text/plain, */*",
            "authorization": f"Bearer {API_TOKEN}",
            "user-agent": "Mozilla/5.0"
        }
        resp = requests.get(API_URL, headers=headers, timeout=30)
        data = resp.json()

        if not isinstance(data, list):
            print("⚠️ API không trả về list hợp lệ")
            return 0

        save_to_db(data)
        print(f"[{datetime.now()}] ✅ Lưu {len(data)} phiên "
              f"({data[0]['issueId']} → {data[-1]['issueId']})")
        return len(data)
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Lỗi fetch:", e)
        return 0

def loop_task():
    init_db()
    while True:
        fetch_and_save()
        time.sleep(INTERVAL)

# ====== FLASK ======
app = Flask(__name__)

@app.route("/")
def home():
    return "🐢 Sicbo Collector Running"

@app.route("/health")
def health():
    return "OK"

if __name__ == "__main__":
    t = threading.Thread(target=loop_task, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
