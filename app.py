#!/usr/bin/env python3
# app.py
import os
import time
import json
import requests
import threading
import psycopg2
from datetime import datetime
from flask import Flask, jsonify

# ===== CONFIG =====
API_URL = os.getenv("API_ENDPOINT", "https://www.vn58q.bet/api/minigame/games/PK3_60S/history100")
API_TOKEN = os.getenv("API_TOKEN")  # required
INTERVAL = int(os.getenv("INTERVAL", "3600"))  # default 3500s as bạn muốn
RETRY_SLEEP = int(os.getenv("RETRY_SLEEP", "5"))  # retry wait on transient errors
DB_DSN = os.getenv("DATABASE_URL")  # required

# ===== UTIL =====
def log(*args, **kwargs):
    print(f"[{datetime.utcnow().isoformat()}] ", *args, **kwargs, flush=True)

# ===== DB helpers =====
def dsn_with_ssl(dsn: str) -> str:
    if not dsn:
        raise ValueError("DATABASE_URL not set")
    if "sslmode" not in dsn:
        if "?" in dsn:
            return dsn + "&sslmode=require"
        else:
            return dsn + "?sslmode=require"
    return dsn

def get_conn():
    dsn = dsn_with_ssl(DB_DSN)
    return psycopg2.connect(dsn, connect_timeout=10)

def init_db():
    log("Khởi tạo DB / tạo bảng sessions_new (nếu chưa có)...")
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
    );
    """)
    conn.commit()
    cur.close()
    conn.close()
    log("✅ Bảng sessions_new đã sẵn sàng")

# ===== save =====
def save_to_db(sessions):
    if not sessions:
        return 0
    inserted = 0
    try:
        conn = get_conn()
        cur = conn.cursor()
        for s in sessions:
            # chuyển đổi: VN58 trả "result" like "413" + issueId; 
            # chúng ta cần parse result -> dice numbers? 
            # Nếu API trả 3-digit string like "413" -> dice = [4,1,3]
            try:
                res_str = s.get("result", "")
                # defensive parse: lấy 3 ký tự cuối hoặc toàn bộ nếu ==3
                if isinstance(res_str, str) and len(res_str) >= 3:
                    dice_chars = res_str[-3:]  # giữ 3 ký tự cuối
                    d1, d2, d3 = int(dice_chars[0]), int(dice_chars[1]), int(dice_chars[2])
                    point = d1 + d2 + d3
                else:
                    # fallback (nếu định dạng khác): try listBetTypeWin? đặt -1
                    d1 = d2 = d3 = 0
                    point = 0
                cur.execute("""
                    INSERT INTO sessions_new (id, dice1, dice2, dice3, point, result)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (int(s.get("issueId", "").split("-")[-1] or 0), d1, d2, d3, point, res_str))
                # Note: issueId dạng PK3_60S-251001-1424 -> trailing number used as id fallback
                inserted += cur.rowcount
            except Exception as e:
                log("❌ Lỗi khi insert 1 row:", e, "row:", s)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        log("❌ Lỗi kết nối DB khi save:", e)
        raise
    return inserted

# ===== fetch & process =====
def fetch_and_save_once():
    if not API_TOKEN:
        log("⚠️ API_TOKEN chưa được set. Bỏ qua lần fetch.")
        return 0
    headers = {
        "accept": "application/json, text/plain, */*",
        "authorization": f"Bearer {API_TOKEN}",
        "device-id": "termux-client",
        "referer": "https://www.vn58q.bet/dice-sicbo?schedulerId=PK3_60S",
        "user-agent": "python-requests/2.x"
    }
    try:
        r = requests.get(API_URL, headers=headers, timeout=30)
        log("🔗 Status:", r.status_code)
        r.raise_for_status()
        data = r.json()
        # VN58 trả 1 list (array) of items
        if isinstance(data, list):
            sessions = data
        elif isinstance(data, dict):
            # nếu họ trả {"list": [...]}
            sessions = data.get("list", [])
        else:
            sessions = []
        log(f"📦 Lấy được {len(sessions)} items (showing first 1):", json.dumps(sessions[:1], ensure_ascii=False))
        # normalize and insert
        inserted = save_to_db(sessions)
        log(f"✅ Đã lưu (hoặc skip conflict) ~{inserted} bản mới")
        return inserted
    except Exception as e:
        log("❌ Lỗi fetch/save:", e)
        return 0

def loop_task():
    # init db once; retry if DB temporarily unreachable
    while True:
        try:
            init_db()
            break
        except Exception as e:
            log("❌ Init DB thất bại, thử lại sau:", e)
            time.sleep(RETRY_SLEEP)

    while True:
        try:
            fetch_and_save_once()
        except Exception as e:
            log("❌ Lỗi không mong muốn trong fetch loop:", e)
        log(f"⏳ Chờ {INTERVAL} giây để fetch lần tiếp theo...\n")
        time.sleep(INTERVAL)

# ===== Flask app =====
app = Flask(__name__)

@app.route("/")
def home():
    return "SICBO collector running 🐢"

@app.route("/health")
def health():
    return "OK"

@app.route("/stats")
def stats():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sessions_new;")
        total = cur.fetchone()[0]
        cur.close()
        conn.close()
        return jsonify({"rows": total})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    t = threading.Thread(target=loop_task, daemon=True)
    t.start()
    port = int(os.getenv("PORT", "10000"))
    log("App starting, port=", port)
    app.run(host="0.0.0.0", port=port)
