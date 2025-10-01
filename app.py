import requests
import os
import time
import psycopg2
from datetime import datetime
from flask import Flask
import threading

# ====== CONFIG ======
API_URL = "https://wtx.tele68.com/v1/tx/sessions"
INTERVAL = 60   # fetch mỗi 60s

# ====== KẾT NỐI DB ======
def get_conn():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("❌ DATABASE_URL chưa được set trong environment")
    # ép buộc sslmode=require
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
    CREATE TABLE IF NOT EXISTS sessions_new (
        id BIGINT PRIMARY KEY,
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
    print("✅ Đã tạo bảng sessions_new (nếu chưa có)", flush=True)

# ====== LƯU DB ======
def save_to_db(new_sessions):
    conn = get_conn()
    cur = conn.cursor()
    for s in new_sessions:
        try:
            cur.execute("""
                INSERT INTO sessions_new (id, dice1, dice2, dice3, point, result)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (s["id"], s["dices"][0], s["dices"][1], s["dices"][2],
                  s["point"], s["resultTruyenThong"]))
        except Exception as e:
            print("❌ Lỗi insert:", e, flush=True)
    conn.commit()
    cur.close()
    conn.close()

# ====== FETCH & SAVE ======
def fetch_and_save():
    try:
        token = os.getenv("API_TOKEN")
        if not token:
            print("⚠️ Thiếu API_TOKEN", flush=True)
            return 0

        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(API_URL, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if "list" not in data:
            print("⚠️ API không trả về dữ liệu hợp lệ", flush=True)
            return 0

        sessions = data["list"]
        sessions.sort(key=lambda x: x["id"])
        save_to_db(sessions)

        print(f"[{datetime.now()}] ✅ Lưu {len(sessions)} phiên "
              f"(ID {sessions[0]['id']} → {sessions[-1]['id']})", flush=True)

        return len(sessions)

    except Exception as e:
        print(f"[{datetime.now()}] ❌ Lỗi fetch:", e, flush=True)
        return 0

# ====== VÒNG LẶP ======
def loop_task():
    init_db()
    while True:
        fetch_and_save()
        print(f"⏳ Chờ {INTERVAL} giây...\n", flush=True)
        time.sleep(INTERVAL)

# ====== FLASK WEB ======
app = Flask(__name__)

@app.route("/")
def home():
    return "App is running 🐢"

@app.route("/health")
def health():
    return "OK"

if __name__ == "__main__":
    # chạy loop_task trong thread riêng
    t = threading.Thread(target=loop_task, daemon=True)
    t.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
