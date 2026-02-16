import sqlite3
from datetime import datetime
import json
import re

_QNUM_RE = re.compile(r"^\s*\d+\s*[\.\)]\s*")

def normalize_question_text(text: str) -> str:
    s = str(text or "").strip()
    s = _QNUM_RE.sub("", s)
    return s.strip()


import uuid

DB_FILE = 'reports.db'


# =============================
# CONNECTION
# =============================
def get_conn():
    return sqlite3.connect(DB_FILE)


# =============================
# INIT DB
# =============================
def init_db():
    conn = get_conn()
    cursor = conn.cursor()

    # users table (keep backward compatible)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            role TEXT NOT NULL,
            name TEXT,
            manager_fi TEXT,
            is_verified INTEGER DEFAULT 0,
            rtp_verified_version INTEGER DEFAULT 0
        )
    ''')

    # reports table; unique per user+date so save_report can replace
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            report_date TEXT,
            report_data TEXT,
            UNIQUE(user_id, report_date)
        )
    ''')

    # table for combined reports that РТП сохраняет (one per rtp+date)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rtp_combined (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rtp_name TEXT,
            report_date TEXT,
            combined_data TEXT,
            UNIQUE(rtp_name, report_date)
        )
    ''')

    # admin-managed MKK questions
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mkk_questions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            q_key TEXT UNIQUE,
            q_text TEXT NOT NULL,
            ord INTEGER NOT NULL
        )
    ''')

    # admin-managed RTP list
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rtps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            ord INTEGER NOT NULL
        )
    ''')

    # simple key/value settings store
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')

    conn.commit()

    # --- migrations for older DBs ---
    cursor.execute("PRAGMA table_info(users)")
    cols = [row[1] for row in cursor.fetchall()]
    if 'is_verified' not in cols:
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN is_verified INTEGER DEFAULT 0")
            conn.commit()
        except Exception:
            pass
    if 'rtp_verified_version' not in cols:
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN rtp_verified_version INTEGER DEFAULT 0")
            conn.commit()
        except Exception:
            pass

    # --- seed defaults if empty ---
    # Import here to avoid circular imports on module load
    try:
        import config  # noqa
    except Exception:
        config = None

    if config:
        # seed questions
        cursor.execute("SELECT COUNT(*) FROM mkk_questions")
        if cursor.fetchone()[0] == 0:
            for i, q in enumerate(getattr(config, "QUESTIONS", [])):
                q_key = q.get("key") or f"q_{i+1}"
                q_text = q.get("question") or ""
                cursor.execute(
                    "INSERT OR IGNORE INTO mkk_questions (q_key, q_text, ord) VALUES (?, ?, ?)",
                    (q_key, q_text, i)
                )
            conn.commit()

        # seed rtps
        cursor.execute("SELECT COUNT(*) FROM rtps")
        if cursor.fetchone()[0] == 0:
            for i, name in enumerate(getattr(config, "RTP_LIST", [])):
                cursor.execute(
                    "INSERT OR IGNORE INTO rtps (name, ord) VALUES (?, ?)",
                    (name, i)
                )
            conn.commit()

        # seed rtp password settings (default same as ADMIN_PASSWORD for smooth rollout)
        if get_setting("rtp_password") is None:
            set_setting("rtp_password", getattr(config, "ADMIN_PASSWORD", ""))
        if get_setting("rtp_password_version") is None:
            set_setting("rtp_password_version", "1")

    conn.close()


# =============================
# SETTINGS
# =============================
def get_setting(key: str):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None


def set_setting(key: str, value: str):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        (key, str(value))
    )
    conn.commit()
    conn.close()


# =============================
# ADMIN: MKK QUESTIONS
# =============================
def get_mkk_questions():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT q_key, q_text, ord FROM mkk_questions ORDER BY ord ASC")
    rows = cursor.fetchall()
    conn.close()
    return [{"key": r[0], "question": r[1], "order": r[2]} for r in rows]


def add_mkk_question(q_text: str):
    q_text = normalize_question_text(q_text)
    if not q_text:
        return None

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(MAX(ord), -1) + 1 FROM mkk_questions")
    next_ord = int(cursor.fetchone()[0] or 0)

    q_key = f"q_{uuid.uuid4().hex[:8]}"
    cursor.execute(
        "INSERT INTO mkk_questions (q_key, q_text, ord) VALUES (?, ?, ?)",
        (q_key, q_text, next_ord)
    )
    conn.commit()
    conn.close()
    return q_key


def update_mkk_question(q_key: str, q_text: str):
    conn = get_conn()
    cursor = conn.cursor()
    q_text = normalize_question_text(q_text)
    cursor.execute("UPDATE mkk_questions SET q_text = ? WHERE q_key = ?", (q_text, q_key))
    conn.commit()
    conn.close()


def delete_mkk_question(q_key: str):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM mkk_questions WHERE q_key = ?", (q_key,))
    # re-pack ordering
    cursor.execute("SELECT q_key FROM mkk_questions ORDER BY ord ASC")
    keys = [r[0] for r in cursor.fetchall()]
    for i, k in enumerate(keys):
        cursor.execute("UPDATE mkk_questions SET ord = ? WHERE q_key = ?", (i, k))
    conn.commit()
    conn.close()


def move_mkk_question(q_key: str, direction: str):
    if direction not in ("up", "down"):
        return
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT q_key, ord FROM mkk_questions ORDER BY ord ASC")
    rows = cursor.fetchall()
    idx = next((i for i, r in enumerate(rows) if r[0] == q_key), None)
    if idx is None:
        conn.close()
        return

    swap_idx = idx - 1 if direction == "up" else idx + 1
    if swap_idx < 0 or swap_idx >= len(rows):
        conn.close()
        return

    k1, o1 = rows[idx]
    k2, o2 = rows[swap_idx]
    cursor.execute("UPDATE mkk_questions SET ord = ? WHERE q_key = ?", (o2, k1))
    cursor.execute("UPDATE mkk_questions SET ord = ? WHERE q_key = ?", (o1, k2))
    conn.commit()
    conn.close()


# =============================
# ADMIN: RTP LIST
# =============================
def get_rtp_list():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT name, ord FROM rtps ORDER BY ord ASC")
    rows = cursor.fetchall()
    conn.close()
    return [r[0] for r in rows]


def add_rtp(name: str):
    name = (name or "").strip()
    if not name:
        return False
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(MAX(ord), -1) + 1 FROM rtps")
    next_ord = int(cursor.fetchone()[0] or 0)
    try:
        cursor.execute("INSERT INTO rtps (name, ord) VALUES (?, ?)", (name, next_ord))
        conn.commit()
        ok = True
    except sqlite3.IntegrityError:
        ok = False
    conn.close()
    return ok


def update_rtp(old_name: str, new_name: str):
    """Переименовать РТП и обновить связанные ссылки (users.manager_fi, users.name для role=rtp, rtp_combined)."""
    new_name = (new_name or "").strip()
    if not new_name:
        return False
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE rtps SET name = ? WHERE name = ?", (new_name, old_name))
        # users: привязки сотрудников
        cursor.execute("UPDATE users SET manager_fi = ? WHERE manager_fi = ?", (new_name, old_name))
        # users: сама учётка РТП
        cursor.execute("UPDATE users SET name = ? WHERE role = ? AND name = ?", (new_name, "rtp", old_name))
        # combined
        cursor.execute("UPDATE rtp_combined SET rtp_name = ? WHERE rtp_name = ?", (new_name, old_name))
        conn.commit()
        ok = True
    except sqlite3.IntegrityError:
        ok = False
    finally:
        conn.close()
    return ok


def delete_rtp(name: str):
    """Удалить РТП из списка. Сотрудников отвязываем (manager_fi = NULL), объединённые отчёты удаляем."""
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM rtps WHERE name = ?", (name,))
    # отвязать сотрудников
    cursor.execute("UPDATE users SET manager_fi = NULL WHERE manager_fi = ?", (name,))
    # удалить объединённые отчёты
    cursor.execute("DELETE FROM rtp_combined WHERE rtp_name = ?", (name,))

    # переупаковать ord
    cursor.execute("SELECT name FROM rtps ORDER BY ord ASC")
    names = [r[0] for r in cursor.fetchall()]
    for i, n in enumerate(names):
        cursor.execute("UPDATE rtps SET ord = ? WHERE name = ?", (i, n))

    conn.commit()
    conn.close()


def move_rtp(name: str, direction: str):
    if direction not in ("up", "down"):
        return
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT name, ord FROM rtps ORDER BY ord ASC")
    rows = cursor.fetchall()
    idx = next((i for i, r in enumerate(rows) if r[0] == name), None)
    if idx is None:
        conn.close()
        return

    swap_idx = idx - 1 if direction == "up" else idx + 1
    if swap_idx < 0 or swap_idx >= len(rows):
        conn.close()
        return

    n1, o1 = rows[idx]
    n2, o2 = rows[swap_idx]
    cursor.execute("UPDATE rtps SET ord = ? WHERE name = ?", (o2, n1))
    cursor.execute("UPDATE rtps SET ord = ? WHERE name = ?", (o1, n2))
    conn.commit()
    conn.close()


# =============================
# RTP PASSWORD (separate)
# =============================
def get_rtp_password():
    return get_setting("rtp_password") or ""


def get_rtp_password_version():
    v = get_setting("rtp_password_version")
    try:
        return int(v)
    except Exception:
        return 1


def set_rtp_password(new_password: str):
    new_password = (new_password or "").strip()
    if not new_password:
        return False
    current_v = get_rtp_password_version()
    set_setting("rtp_password", new_password)
    set_setting("rtp_password_version", str(current_v + 1))
    return True


def set_user_rtp_verified_version(user_id: int, version: int):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET rtp_verified_version = ? WHERE user_id = ?", (int(version), int(user_id)))
    conn.commit()
    conn.close()


def get_user_rtp_verified_version(user_id: int):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT rtp_verified_version FROM users WHERE user_id = ?", (int(user_id),))
    row = cursor.fetchone()
    conn.close()
    try:
        return int(row[0]) if row else 0
    except Exception:
        return 0


def is_user_rtp_verified(user_id: int):
    return get_user_rtp_verified_version(user_id) >= get_rtp_password_version()


# =============================
# USERS
# =============================
def add_user(user_id, role, name=None, manager_fi=None):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        'INSERT OR REPLACE INTO users (user_id, role, name, manager_fi) VALUES (?, ?, ?, ?)',
        (user_id, role, name, manager_fi)
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

def delete_user(user_id: int) -> None:
    """Полностью удаляет пользователя и его отчёты (использовать аккуратно)."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM reports WHERE user_id=?", (user_id,))
    cur.execute("DELETE FROM users WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()



def get_manager_id_by_fi(manager_fi):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT user_id FROM users WHERE role = "rtp" AND name = ?', (manager_fi,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None


# =============================
# REPORTS
# =============================
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


# =============================
# RTP COMBINED
# =============================
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


# =============================
# AUTH (RM/ADMIN)
# =============================
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


def get_user_by_name(name):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, role, name, manager_fi FROM users WHERE name = ?", (name,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    return {"user_id": row[0], "role": row[1], "name": row[2], "manager_fi": row[3]}


# initialize DB on import
init_db()
