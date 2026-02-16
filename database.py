import sqlite3
from datetime import datetime
import json

DB_FILE = 'reports.db'

def get_conn():
    return sqlite3.connect(DB_FILE)

def init_db():
    conn = get_conn()
    cursor = conn.cursor()

    # users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            role TEXT NOT NULL,
            name TEXT,
            manager_fi TEXT,
            is_verified INTEGER DEFAULT 0
        )
    ''')

    # reports table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            report_date TEXT,
            report_data TEXT,
            UNIQUE(user_id, report_date)
        )
    ''')

    # combined report from RTP to RM
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rtp_combined (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rtp_name TEXT,
            report_date TEXT,
            combined_data TEXT,
            UNIQUE(rtp_name, report_date)
        )
    ''')

    # key-value app config (admin runtime settings)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS app_config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')

    conn.commit()

    # ensure is_verified column exists
    cursor.execute("PRAGMA table_info(users)")
    cols = [row[1] for row in cursor.fetchall()]
    if 'is_verified' not in cols:
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN is_verified INTEGER DEFAULT 0")
            conn.commit()
        except Exception:
            pass

    conn.close()

# -------------------------
# Users basic
# -------------------------
def add_user(user_id, role, name=None, manager_fi=None):
    conn = get_conn()
    cursor = conn.cursor()
    if name and manager_fi:
        cursor.execute(
            'INSERT OR REPLACE INTO users (user_id, role, name, manager_fi) VALUES (?, ?, ?, ?)',
            (user_id, role, name, manager_fi)
        )
    elif name:
        cursor.execute(
            'INSERT OR REPLACE INTO users (user_id, role, name) VALUES (?, ?, ?)',
            (user_id, role, name)
        )
    else:
        cursor.execute(
            'INSERT OR REPLACE INTO users (user_id, role) VALUES (?, ?)',
            (user_id, role)
        )
    conn.commit()
    conn.close()

def get_user_role(user_id):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT role FROM users WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def get_user_name(user_id):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT name FROM users WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def set_user_name(user_id, name):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET name = ? WHERE user_id = ?', (name, user_id))
    conn.commit()
    conn.close()

def get_manager_fi_for_employee(user_id):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT manager_fi FROM users WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def set_manager_fi_for_employee(user_id, manager_fi):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET manager_fi = ? WHERE user_id = ?', (manager_fi, user_id))
    conn.commit()
    conn.close()

def get_manager_id_by_fi(manager_fi):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT user_id FROM users WHERE role = "rtp" AND name = ?', (manager_fi,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

# -------------------------
# Reports
# -------------------------
def save_report(user_id, report_data):
    date = datetime.now().strftime('%Y-%m-%d')
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        'INSERT OR REPLACE INTO reports (user_id, report_date, report_data) VALUES (?, ?, ?)',
        (user_id, date, json.dumps(report_data, ensure_ascii=False))
    )
    conn.commit()
    conn.close()

def get_report(user_id, date):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT report_data FROM reports WHERE user_id = ? AND report_date = ?', (user_id, date))
    result = cursor.fetchone()
    conn.close()
    return json.loads(result[0]) if result else None

def get_all_reports_on_date(date, manager_fi=None):
    conn = get_conn()
    cursor = conn.cursor()
    if manager_fi:
        cursor.execute('''
            SELECT r.user_id, r.report_data
            FROM reports r
            JOIN users u ON r.user_id = u.user_id
            WHERE r.report_date = ? AND u.manager_fi = ?
        ''', (date, manager_fi))
    else:
        cursor.execute('SELECT user_id, report_data FROM reports WHERE report_date = ?', (date,))
    results = cursor.fetchall()
    conn.close()
    return [(uid, json.loads(data)) for uid, data in results]

def get_employees(manager_fi=None):
    conn = get_conn()
    cursor = conn.cursor()
    if manager_fi:
        cursor.execute("SELECT user_id, name FROM users WHERE role = 'mkk' AND manager_fi = ?", (manager_fi,))
    else:
        cursor.execute("SELECT user_id, name FROM users WHERE role = 'mkk'")
    results = cursor.fetchall()
    conn.close()
    return results

# -------------------------
# Combined RTP reports
# -------------------------
def save_rtp_combined(rtp_name, combined_data, date):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        'INSERT OR REPLACE INTO rtp_combined (rtp_name, report_date, combined_data) VALUES (?, ?, ?)',
        (rtp_name, date, json.dumps(combined_data, ensure_ascii=False))
    )
    conn.commit()
    conn.close()

def get_rtp_combined(rtp_name, date):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT combined_data FROM rtp_combined WHERE rtp_name = ? AND report_date = ?', (rtp_name, date))
    row = cursor.fetchone()
    conn.close()
    return json.loads(row[0]) if row else None

def get_all_rtp_combined_on_date(date):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT rtp_name, combined_data FROM rtp_combined WHERE report_date = ?', (date,))
    rows = cursor.fetchall()
    conn.close()
    return [(r[0], json.loads(r[1])) for r in rows]

def get_rtp_combined_status_for_all(rtp_list, date):
    conn = get_conn()
    cursor = conn.cursor()
    result = {}
    for r in rtp_list:
        cursor.execute('SELECT 1 FROM rtp_combined WHERE rtp_name = ? AND report_date = ?', (r, date))
        row = cursor.fetchone()
        result[r] = bool(row)
    conn.close()
    return result

# -------------------------
# Authorization (password remembered)
# -------------------------
def set_user_verified(user_id, val=1):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET is_verified = ? WHERE user_id = ?', (1 if val else 0, user_id))
    conn.commit()
    conn.close()

def is_user_verified(user_id):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT is_verified FROM users WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    conn.close()
    return bool(row[0]) if row else False

# helper: find user by name
def get_user_by_name(name):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, role, name, manager_fi FROM users WHERE name = ?", (name,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    return {"user_id": row[0], "role": row[1], "name": row[2], "manager_fi": row[3]}

# -------------------------
# App config (admin editable)
# -------------------------
def set_app_config(key: str, value):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('INSERT OR REPLACE INTO app_config (key, value) VALUES (?, ?)', (key, json.dumps(value, ensure_ascii=False)))
    conn.commit()
    conn.close()

def get_app_config(key: str, default=None):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT value FROM app_config WHERE key = ?', (key,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return default
    try:
        return json.loads(row[0])
    except Exception:
        return default

# -------------------------
# Admin: RTP list CRUD (stored in DB)
# -------------------------
RTP_LIST_KEY = "rtp_list"
RTP_PASSWORD_KEY = "rtp_password"
MKK_QUESTIONS_KEY = "mkk_questions"

def get_rtp_list(default_list):
    return get_app_config(RTP_LIST_KEY, default_list) or default_list

def set_rtp_list(rtps):
    set_app_config(RTP_LIST_KEY, rtps)

def get_rtp_password(default_pwd):
    return get_app_config(RTP_PASSWORD_KEY, default_pwd) or default_pwd

def set_rtp_password(pwd):
    set_app_config(RTP_PASSWORD_KEY, pwd)

def get_mkk_questions(default_questions):
    return get_app_config(MKK_QUESTIONS_KEY, default_questions) or default_questions

def set_mkk_questions(questions):
    set_app_config(MKK_QUESTIONS_KEY, questions)

# -------------------------
# Admin: Employees editor (MKK users by RTP)
# -------------------------
def get_employees_by_rtp(rtp_fi: str):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT user_id, name, manager_fi FROM users WHERE role = 'mkk' AND manager_fi = ? ORDER BY name",
        (rtp_fi,)
    )
    rows = cursor.fetchall()
    conn.close()
    return [{"user_id": r[0], "name": r[1], "manager_fi": r[2]} for r in rows]

def delete_user(user_id: int):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

def clear_employee_manager(user_id: int):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET manager_fi = NULL WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

# initialize DB on import
init_db()
