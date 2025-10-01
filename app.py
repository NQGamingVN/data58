import os
import time
import threading
import requests
import psycopg2
from psycopg2.extras import Json
from flask import Flask
from datetime import datetime

# ====== CONFIG ======
DB_URL = os.getenv("DATABASE_URL")  # bao gồm ?sslmode=require nếu cần
API_URL = "https://www.vn58q.bet/api/minigame/games/PK3_60S/history100"
API_TOKEN = os.getenv("API_TOKEN", "")
DEVICE_ID = os.getenv("DEVICE_ID", "")

INTERVAL = 3600  

# ====== DB ======
def get_conn():
    # psycopg2 tự hiểu sslmode nếu có trong DB_URL
    return psycopg2.connect(DB_URL)

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sessions_new (
        issue_id TEXT PRIMARY KEY,
        dice1 INT,
        dice2 INT,
        dice3 INT,
        point INT,
        result TEXT,
        listbet JSONB,
        open_time TIMESTAMPTZ DEFAULT NOW()
    );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Bảng sessions_new đã sẵn sàng", flush=True)

# ====== HELPERS ======
def parse_result_str(s):
    # s like "115", 3 ký tự
    return int(s[0]), int(s[1]), int(s[2])

def calc_tai_xiu(pt):
    return "TAI" if pt >= 11 else "XIU"

# ====== FETCH & SAVE ======
def fetch_and_save():
    if not API_TOKEN or not DEVICE_ID:
        print("⚠️ Thiếu API_TOKEN hoặc DEVICE_ID", flush=True)
        return

    headers = {
        "accept": "application/json, text/plain, */*",
        "authorization": f"Bearer {API_TOKEN}",
        "device-id": DEVICE_ID
    }

    try:
        resp = requests.get(API_URL, headers=headers, timeout=30)
        resp.raise_for_status()
        obj = resp.json()

        data_list = obj.get("data")
        if not isinstance(data_list, list):
            print("⚠️ Dữ liệu trả về không đúng định dạng:", obj, flush=True)
            return

        conn = get_conn()
        cur = conn.cursor()
        count = 0
        for item in data_list:
            issue_id = item.get("issueId")
            res_str = item.get("result")
            listbet = item.get("listBetTypeWin", [])

            # parse
            try:
                d1, d2, d3 = parse_result_str(res_str)
            except Exception as e:
                print("⚠️ Không parse được result_str:", res_str, e, flush=True)
                continue

            pt = d1 + d2 + d3
            tx = calc_tai_xiu(pt)

            cur.execute("""
                INSERT INTO sessions_new (issue_id, dice1, dice2, dice3, point, result, listbet)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (issue_id) DO NOTHING
            """, (issue_id, d1, d2, d3, pt, tx, Json(listbet)))

            count += 1

        conn.commit()
        cur.close()
        conn.close()
        print(f"[{datetime.now()}] ✅ Lưu {count} phiên", flush=True)

    except Exception as e:
        print(f"[{datetime.now()}] ❌ Lỗi fetch/save:", e, flush=True)

def loop_task():
    init_db()
    while True:
        fetch_and_save()
        time.sleep(INTERVAL)

# ====== FLASK web ======
app = Flask(__name__)

@app.route("/")
def home():
    return "VN58 Sicbo Collector Running"

@app.route("/health")
def health():
    return "OK"

if __name__ == "__main__":
    t = threading.Thread(target=loop_task, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
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
