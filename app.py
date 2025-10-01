# app.py
import os
import time
import requests
import psycopg2
from datetime import datetime
from flask import Flask
import threading

# ===== CONFIG =====
API_URL = "https://www.vn58q.bet/api/minigame/games/PK3_60S/history100"
INTERVAL = 3600  # giây

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

# ===== Fetch & save =====
API_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImFKTHhUajhxOU1ZMWlkbHZaZHIyTyIsInR5cCI6IkpXVCJ9.eyJhbHRJZCI6NDM3NjI3MiwiYXVkIjpbImxvdG8iXSwiY2x0IjoxMDU4LCJleHAiOjE3NTk0MTY3ODksImd0eSI6InBhc3N3b3JkIiwiaWF0IjoxNzU5MzMwMzc5LCJpc3MiOiJodHRwczovL3ZuNTguanAuYXV0aDAuY29tLyIsInN1YiI6IjY3ZmY3YmJmOGY3MDAwNDZhNzUxNmIxZCIsInVzZXJUeXBlIjoiQUdFTlQiLCJ1c2VybmFtZSI6InF1YW5nbm9ybWFsIn0.dyUgAsYsBBc9SJ1jU9-xuB0D-rj6bpB_HMxjtL0vBjv0Nkod2wqUcJv8jtSxxgjZoZcRSETFQREMR5MmlQaqds4shn4Rd5fxW7eSHRBHh9m66h3usvyzaTqW4coRpkQFTkQmP0vAdZGpREQ7NaIubMwBlH-nmKRtazgkTRT1Q16ZEgdDtXgR_-o-nfpZcuFVvbaoHiwFFeuLjwgPhX7VG9uVTwW5GDqFM-LVR6X7IpEINRbgLrXONDWO2C3zP8VpGB4z4EJ7XeXl4hHPJ0BRxDJRbEavVSt-aX8yyKWUq7hEZPT_OO6SHEpimjRPFqb125DWOuMRpQ4siZ4JLDR2_YL0AoKccfAmmqClLNZwHXAepv8ZctWtNHNaAYaR_mD-pd9ER2JBdrR6Km2Afp_azdNmoCmrib-yYaw8TJqpo93h4aMEZ44bHDCO43VqaC2Fp2MXuKF2o7viniOT_-tLnUfV6mhqhZsoYqofqND6hlA5RnX-1QujqHdmzsRpZ9j0OPuG3innOV4eZ1jSIvrpHoEZBNnv-d3wcjuq-d55PaCoYt2MQPfCHt0tIysFm5Z4nFwPAtaypSXpWpbciNoBBZs7-z3xiB9euNE0TRrrsYOkGKaI53gCM4DRSEacKORp-RjWEJT9JzGnQ1C5XCaLPXPJipJZW7c--gejdU9ntKk"
DEVICE_ID = "b01c2bec8afd532578f3b73ae748082d"

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
    headers = {
        "accept": "application/json, text/plain, */*",
        "authorization": f"Bearer {API_TOKEN}",
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
