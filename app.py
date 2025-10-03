import os
import requests
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime

# ================== CONFIG ==================
DB_HOST = "aws-1-ap-southeast-1.pooler.supabase.com"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_USER = "postgres.oyksgzdnbmdgqosxosid"
DB_PASS = "He4YjRjFdK5k7Mno"

LOGIN_URL = "https://www.vn58q.bet/api/account/login"
DATA_URL = "https://www.vn58q.bet/api/sessions"

USERNAME = "quangnormal"
PASSWORD = "12345abC_"
DEVICE_ID = "86df014ab3a79d197a9e394428ba73a1"

# ================== DB ==================
def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def init_db():
    with get_db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions_new (
            id BIGINT PRIMARY KEY,
            created_at TIMESTAMP,
            user_id BIGINT,
            amount NUMERIC,
            status TEXT
        );
        """)
        conn.commit()

# ================== LOGIN ==================
def get_token():
    try:
        data = {
            "username": USERNAME,
            "password": PASSWORD,
            "siteKey": "6LfR_pYpAAAAAN20hVh1-AaBbVuf4oN7e4JU91dt",
            "captcha": ""   # để trống nếu không cần captcha
        }
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/x-www-form-urlencoded",
            "device-id": DEVICE_ID,
            "origin": "https://www.vn58q.bet",
            "referer": "https://www.vn58q.bet/login",
            "user-agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36",
        }
        resp = requests.post(LOGIN_URL, data=data, headers=headers, timeout=30)
        resp.raise_for_status()
        js = resp.json()
        print(f"📥 Login response: {js}", flush=True)

        token = (
            js.get("idToken")
            or js.get("accessToken")
            or js.get("access_token")
            or js.get("token")
            or js.get("data", {}).get("idToken")
            or js.get("data", {}).get("accessToken")
            or js.get("data", {}).get("access_token")
        )

        if not token:
            raise Exception(f"Không lấy được token từ response: {js}")

        print(f"🔑 Lấy token thành công", flush=True)
        return token
    except Exception as e:
        print(f"❌ Lỗi login: {e}", flush=True)
        return None

# ================== FETCH DATA ==================
def fetch_data(token):
    try:
        headers = {
            "accept": "application/json, text/plain, */*",
            "authorization": f"Bearer {token}",
            "user-agent": "Mozilla/5.0",
        }
        resp = requests.get(DATA_URL, headers=headers, timeout=30)
        resp.raise_for_status()
        js = resp.json()
        return js.get("data", [])
    except Exception as e:
        print(f"❌ Lỗi fetch data: {e}", flush=True)
        return []

# ================== SAVE DB ==================
def save_to_db(rows):
    with get_db_conn() as conn, conn.cursor() as cur:
        for row in rows:
            try:
                cur.execute("""
                    INSERT INTO sessions_new (id, created_at, user_id, amount, status)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                """, (
                    row.get("id"),
                    datetime.fromtimestamp(row.get("createdAt")/1000) if row.get("createdAt") else None,
                    row.get("userId"),
                    row.get("amount"),
                    row.get("status"),
                ))
            except Exception as e:
                print(f"⚠️ Lỗi insert row {row}: {e}", flush=True)
        conn.commit()

# ================== MAIN ==================
def main():
    init_db()
    token = get_token()
    if not token:
        print("❌ Không có token, dừng lại.", flush=True)
        return

    rows = fetch_data(token)
    if not rows:
        print("❌ Không lấy được dữ liệu", flush=True)
        return

    print(f"📊 Lấy {len(rows)} dòng dữ liệu", flush=True)
    save_to_db(rows)
    print("✅ Đã lưu vào DB", flush=True)

if __name__ == "__main__":
    main()def save_rows(rows):
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

# ===== Fetch & save =====
def fetch_and_save():
    token = get_token()
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
    app.run(host="0.0.0.0", port=port)
