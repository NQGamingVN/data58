# app.py
import os
import time
import requests
import psycopg2
from datetime import datetime
from flask import Flask
import threading

# ===== CONFIG =====
LOGIN_URL = "https://www.vn58q.bet/api/account/login"
API_URL = "https://www.vn58q.bet/api/minigame/games/PK3_60S/history100"
INTERVAL = 3600  # giây

USERNAME = "quangnormal"
PASSWORD = "12345abC_"
DEVICE_ID = "b01c2bec8afd532578f3b73ae748082d"

# ===== DB helper =====
DATABASE_URL = "postgresql://postgres.yqtvaxgthwqjegjouxko:12345abC_MatKhau@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres?sslmode=require"

def get_conn():
    retries = 5
    for i in range(retries):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = True
            print(f"✅ Kết nối DB thành công (attempt {i+1})", flush=True)
            return conn
        except Exception as e:
            print(f"❌ Kết nối DB thất bại ({i+1}/{retries}): {e}", flush=True)
            time.sleep(5)
    raise Exception("Không thể kết nối DB")

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sessions_new (
        issue_id TEXT PRIMARY KEY,
        dice1 SMALLINT,
        dice2 SMALLINT,
        dice3 SMALLINT,
        point SMALLINT,
        result_text TEXT,
        raw_result TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    )
    """)
    cur.close()
    conn.close()
    print("✅ Bảng sessions_new đã sẵn sàng", flush=True)

# ===== Parse helper =====
def parse_result_string(r):
    if not r:
        return None
    r = str(r).strip()
    digits = [ch for ch in r if ch.isdigit()]
    if len(digits) >= 3:
        d1, d2, d3 = int(digits[0]), int(digits[1]), int(digits[2])
        return d1, d2, d3
    parts = [p for p in r.replace(',', ':').split(':') if p.strip().isdigit()]
    if len(parts) >= 3:
        return int(parts[0]), int(parts[1]), int(parts[2])
    return None

def point_to_text(point):
    return "TAI" if point >= 11 else "XIU"

# ===== Login helper =====
def get_api_token():
    login_data = {
        "username": USERNAME,
        "password": PASSWORD,
        "siteKey": "6LfR_pYpAAAAAN20hVh1-AaBbVuf4oN7e4JU91dt",
        "captcha": "0"   # bypass, nếu server không bắt buộc captcha
    }
    headers = {
        "content-type": "application/x-www-form-urlencoded",
        "device-id": DEVICE_ID,
        "referer": "https://www.vn58q.bet/login",
        "accept": "application/json, text/plain, */*",
        "user-agent": "Mozilla/5.0"
    }
    try:
        r = requests.post(LOGIN_URL, data=login_data, headers=headers, timeout=15)
        r.raise_for_status()
        token = r.json().get("access_token")
        if not token:
            raise Exception("Không lấy được access_token")
        return token
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Lỗi login: {e}", flush=True)
        return None

# ===== Fetch & save =====
def save_rows(rows):
    if not rows:
        return 0
    conn = get_conn()
    cur = conn.cursor()
    inserted = 0
    for r in rows:
        try:
            issue_id = r.get("issueId") or r.get("issue_id")
            raw_result = r.get("result")
            parsed = parse_result_string(raw_result)
            if not issue_id or not parsed:
                print(f"[{datetime.now()}] ⚠️ Bỏ qua row: {r}", flush=True)
                continue
            dice1, dice2, dice3 = parsed
            point = dice1 + dice2 + dice3
            result_text = point_to_text(point)
            cur.execute("""
                INSERT INTO sessions_new (issue_id, dice1, dice2, dice3, point, result_text, raw_result)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (issue_id) DO NOTHING
            """, (issue_id, dice1, dice2, dice3, point, result_text, str(raw_result)))
            inserted += 1
        except Exception as e:
            print(f"[{datetime.now()}] ❌ Lỗi insert row: {e}", flush=True)
    conn.commit()
    cur.close()
    conn.close()
    return inserted

def fetch_and_save():
    token = get_api_token()
    if not token:
        return 0
    headers = {
        "accept": "application/json, text/plain, */*",
        "authorization": f"Bearer {token}",
        "device-id": DEVICE_ID,
    }
    try:
        resp = requests.get(API_URL, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        rows = data if isinstance(data, list) else data.get("list") or data.get("data") or []
        saved = save_rows(rows)
        print(f"[{datetime.now()}] 🔗 Status: {resp.status_code}, Lưu/attempted {saved}/{len(rows)} rows", flush=True)
        return saved
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Lỗi fetch/save: {e}", flush=True)
        return 0

# ===== Loop task =====
def loop_task():
    init_db()
    while True:
        fetch_and_save()
        print(f"⏳ Chờ {INTERVAL} giây để fetch lần tiếp theo...\n", flush=True)
        time.sleep(INTERVAL)

# ===== Flask =====
app = Flask(__name__)

@app.route("/")
def home():
    return "vn58 collector is running 🐢"

@app.route("/health")
def health():
    return "OK"

if __name__ == "__main__":
    t = threading.Thread(target=loop_task, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)        "authorization": f"Bearer {API_TOKEN}",
        "device-id": DEVICE_ID,
    }
    try:
        resp = requests.get(API_URL, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        rows = data if isinstance(data, list) else data.get("list") or data.get("data") or []
        saved = save_rows(rows)
        print(f"[{datetime.now()}] 🔗 Status: {resp.status_code}, Lưu/attempted {saved}/{len(rows)} rows", flush=True)
        return saved
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Lỗi fetch/save: {e}", flush=True)
        return 0

# ===== Loop task =====
def loop_task():
    init_db()
    while True:
        fetch_and_save()
        print(f"⏳ Chờ {INTERVAL} giây để fetch lần tiếp theo...\n", flush=True)
        time.sleep(INTERVAL)

# ===== Flask =====
app = Flask(__name__)

@app.route("/")
def home():
    return "vn58 collector is running 🐢"

@app.route("/health")
def health():
    return "OK"

if __name__ == "__main__":
    t = threading.Thread(target=loop_task, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
